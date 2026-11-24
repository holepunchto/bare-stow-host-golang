// Package stow is the host side of the bare-stow lifecycle. It multiplexes a
// control channel and a user data channel over a byte transport to a stowed
// bundle, and layers readiness, termination, and exit reporting on top.
package stow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"
)

// DefaultGrace is the default time a terminating worker gets to stop and exit.
const DefaultGrace = 5 * time.Second

// Options configures Wrap. The zero value holds the defaults.
type Options struct {
	// Kill stops a worker that has ignored a terminate control frame for the
	// grace period. Defaults to closing the transport.
	Kill func()
	// Timeout is how long to wait for the worker to signal ready before
	// failing Ready and terminating it. Zero waits indefinitely.
	Timeout time.Duration
	// Grace is how long a terminating worker gets to stop and exit before
	// Kill is called. Zero means DefaultGrace.
	Grace time.Duration
}

// An IPC is the host side of a stowed bundle, layering readiness,
// termination, and exit reporting over a transport.
//
// Reading yields the user data written by the worker until the transport
// ends, and fails with a *WorkerError once the worker has reported a fault or
// exited without saying why. Writing sends user data to the worker, and fails
// with a WORKER_CLOSED *WorkerError once the transport has ended. A transport
// that is reset or broken by the worker going away counts as ending rather
// than failing. One goroutine may read while others write.
//
// Whoever wraps a transport must report the exit of the worker with Exited.
// The end of a transport that ends without the worker saying why, or being
// asked to terminate, is held back until then, so that reads, Ready, Wait, and
// closing fail with the reason instead of ending cleanly. A transport that
// fails instead waits for the exit for the grace period, failing with the exit
// wrapping the failure, or calling Kill and failing with TRANSPORT_ERROR if the
// worker hasn't exited by then.
type IPC struct {
	transport io.ReadWriteCloser
	kill      func()
	grace     time.Duration

	writing sync.Mutex

	mu           sync.Mutex
	changed      chan struct{}
	readySettled bool
	readyErr     error
	data         [][]byte
	ended        bool
	closed       bool
	exitCode     int
	exitReported bool
	failure      *WorkerError
	pending      *WorkerError
	terminating  bool
	deadline     time.Time
	killed       bool
}

// Wrap wraps the host side of a stowed bundle's transport and sends the grace
// period to the worker in a configure control frame. A nil opts holds the
// defaults.
func Wrap(transport io.ReadWriteCloser, opts *Options) *IPC {
	var o Options

	if opts != nil {
		o = *opts
	}

	if o.Kill == nil {
		o.Kill = func() { transport.Close() }
	}

	if o.Grace <= 0 {
		o.Grace = DefaultGrace
	}

	x := &IPC{
		transport: transport,
		kill:      o.Kill,
		grace:     o.Grace,
		changed:   make(chan struct{}),
		exitCode:  -1,
	}

	grace := o.Grace.Milliseconds()

	// The lock is taken here, so that the configure frame precedes any user
	// data, but released by the goroutine, so that Wrap never blocks on a
	// transport that the worker isn't reading yet.
	x.writing.Lock()

	go func() {
		_, err := transport.Write(encodeControl(control{Type: "configure", Grace: &grace}))

		x.writing.Unlock()
		x.onWrite(err)
	}()

	go x.readFrames()

	if o.Timeout > 0 {
		go x.timeOut(o.Timeout)
	}

	return x
}

// Ready waits for the worker to signal ready, or for ctx to be done.
//
// It fails with a *WorkerError if the worker errors, exits, closes, or times
// out before then, in which case the worker is terminated. The worker is also
// terminated if ctx is done before then.
func (x *IPC) Ready(ctx context.Context) error {
	x.mu.Lock()

	settled := x.waitWhile(ctx.Done(), func() bool { return !x.readySettled })
	err := x.readyErr

	x.mu.Unlock()

	if !settled {
		go x.close()

		return ctx.Err()
	}

	return err
}

// ExitCode returns the exit code the worker reported, or -1 if none.
func (x *IPC) ExitCode() int {
	x.mu.Lock()
	defer x.mu.Unlock()

	return x.exitCode
}

// Wait waits for the transport to end and returns the exit code the worker
// reported, or -1 if none.
func (x *IPC) Wait() int {
	x.mu.Lock()
	defer x.mu.Unlock()

	x.waitWhile(nil, func() bool { return !x.closed })

	return x.exitCode
}

// Terminate asks the worker to terminate and waits for the transport to end,
// calling Kill if the worker hasn't exited within the grace period. It
// returns the exit code the worker reported, or -1 if none.
func (x *IPC) Terminate() int {
	x.close()

	return x.ExitCode()
}

// Close is like Terminate, without the exit code. It always returns nil.
func (x *IPC) Close() error {
	x.close()

	return nil
}

// Exited reports that the worker has exited, as observed by the transport,
// such as the process of a sidecar. It must be called once the worker exits. code is the exit code, or -1 if none, and
// signal is the signal that killed the worker, if positive.
//
// If the worker exited without saying why and was not asked to, the handle
// fails with a WORKER_EXITED or WORKER_KILLED *WorkerError carrying ExitCode
// and Signal.
func (x *IPC) Exited(code int, signal syscall.Signal) {
	x.mu.Lock()

	if x.exitReported || x.terminating {
		x.mu.Unlock()

		return
	}

	pending := x.pending

	x.mu.Unlock()

	var err *WorkerError

	if signal > 0 {
		err = newError(CodeWorkerKilled, fmt.Sprintf("Worker was killed by signal %d", int(signal)))
		err.Signal = signal
	} else {
		err = newError(CodeWorkerExited, fmt.Sprintf("Worker exited with code %d", code))
	}

	err.ExitCode = code

	if pending != nil {
		err.err = pending
	}

	x.fail(err)
}

// Read reads user data written by the worker, returning io.EOF once the
// transport has ended.
func (x *IPC) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	x.waitWhile(nil, func() bool {
		return len(x.data) == 0 && x.failure == nil && !x.closed
	})

	if len(x.data) > 0 {
		n := copy(b, x.data[0])

		if n == len(x.data[0]) {
			x.data = x.data[1:]
		} else {
			x.data[0] = x.data[0][n:]
		}

		return n, nil
	}

	if x.failure != nil {
		return 0, x.failure
	}

	return 0, io.EOF
}

// Write sends b to the worker as user data.
func (x *IPC) Write(b []byte) (int, error) {
	if err := x.send(encodeFrame(tagUser, b)); err != nil {
		return 0, err
	}

	return len(b), nil
}

// waitWhile waits, with x.mu held, until cond no longer holds or done is
// closed. It reports whether cond no longer holds.
func (x *IPC) waitWhile(done <-chan struct{}, cond func() bool) bool {
	for cond() {
		changed := x.changed

		x.mu.Unlock()

		select {
		case <-changed:
			x.mu.Lock()
		case <-done:
			x.mu.Lock()

			return !cond()
		}
	}

	return true
}

func (x *IPC) notify() {
	close(x.changed)
	x.changed = make(chan struct{})
}

func (x *IPC) send(frame []byte) error {
	x.writing.Lock()
	_, err := x.transport.Write(frame)
	x.writing.Unlock()

	return x.onWrite(err)
}

// A write that finds the worker gone leaves the end to the reader, so that
// what the worker sent before it went, such as its exit frame, isn't lost.
func (x *IPC) onWrite(err error) error {
	if err == nil {
		return nil
	}

	if failure := x.onTransportError(err); failure != nil {
		return failure
	}

	return closedError()
}

// A worker that may have crashed is likely why its transport failed, so its
// exit is waited for as an end is, killing it if it hasn't exited within the
// grace period. Returns the failure, if the error is one.
func (x *IPC) onTransportError(err error) *WorkerError {
	if isEnd(err) {
		return nil
	}

	failure := transportError(err)

	x.mu.Lock()

	// The worker already ended, said why it exits, or was asked to.
	if x.ended || x.exitReported || x.terminating {
		x.mu.Unlock()

		return nil
	}

	if x.mayHaveCrashed() {
		if x.pending == nil {
			x.pending = failure
			x.notify()

			go x.failAfterGrace(failure)
		}

		x.mu.Unlock()

		return failure
	}

	x.mu.Unlock()

	x.fail(failure)

	return failure
}

// The failure is recorded before the worker is killed, so that an exit that
// the kill causes is ignored.
func (x *IPC) failAfterGrace(failure *WorkerError) {
	ctx, cancel := context.WithTimeout(context.Background(), x.grace)
	defer cancel()

	x.mu.Lock()

	if x.waitWhile(ctx.Done(), func() bool { return x.mayHaveCrashed() && !x.closed }) {
		x.mu.Unlock()

		return
	}

	x.recordFailure(failure)

	x.mu.Unlock()

	x.killOnce()
	x.afterFailure(failure)
}

func (x *IPC) mayHaveCrashed() bool {
	return !x.exitReported && !x.terminating && x.failure == nil
}

func (x *IPC) timeOut(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	x.mu.Lock()

	// A worker whose transport has ended or failed can no longer time out.
	timedOut := !x.waitWhile(ctx.Done(), func() bool {
		return !x.readySettled && !x.ended && x.pending == nil
	})

	x.mu.Unlock()

	if timedOut {
		x.settleReady(newError(CodeWorkerTimeout, "Worker timed out before ready"))
	}
}

func (x *IPC) readFrames() {
	var d decoder

	buf := make([]byte, 65536)

	for {
		n, err := x.transport.Read(buf)

		for _, f := range d.feed(buf[:n]) {
			x.onFrame(f)
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				x.onTransportError(err)
			}

			break
		}
	}

	x.transport.Close()
	x.onClose()
}

func (x *IPC) onFrame(f frame) {
	switch f.tag {
	case tagUser:
		x.mu.Lock()

		if x.failure == nil && len(f.payload) > 0 {
			x.data = append(x.data, f.payload)
			x.notify()
		}

		x.mu.Unlock()
	case tagControl:
		m, ok := decodeControl(f.payload)

		if !ok {
			return
		}

		switch m.Type {
		case "ready":
			x.settleReady(nil)
		case "exit":
			code := -1

			if m.Code != nil {
				code = *m.Code
			}

			x.mu.Lock()

			x.exitReported = true
			x.exitCode = code
			x.notify()

			faulted := x.failure != nil

			x.mu.Unlock()

			// An error frame precedes the exit frame of a worker that faulted.
			if !faulted {
				err := newError(CodeWorkerExited, fmt.Sprintf("Worker exited with code %d before ready", code))
				err.ExitCode = code

				x.settleReady(err)
			}
		case "error":
			err := newError(CodeWorkerError, m.Message)
			err.Stack = m.Stack

			x.fail(err)
		}
	}
}

// A worker whose transport ends without it saying why or being asked to has
// crashed, so the end is held back until its exit is reported for readers to
// fail with. A worker that is already failing settles ready with its failure.
func (x *IPC) onClose() {
	x.mu.Lock()

	x.ended = true

	if x.mayHaveCrashed() {
		x.mu.Unlock()

		return
	}

	x.closed = true
	x.notify()

	failing := x.failure != nil

	x.mu.Unlock()

	if !failing {
		x.settleReady(newError(CodeWorkerClosed, "Worker closed before ready"))
	}
}

func (x *IPC) fail(err *WorkerError) {
	x.mu.Lock()

	recorded := x.recordFailure(err)

	x.mu.Unlock()

	if recorded {
		x.afterFailure(err)
	}
}

func (x *IPC) recordFailure(err *WorkerError) bool {
	if x.failure != nil {
		return false
	}

	x.failure = err
	x.closed = x.ended
	x.notify()

	return true
}

func (x *IPC) afterFailure(err *WorkerError) {
	if !x.settleReady(err) {
		go x.close()
	}
}

// Settling with an error terminates the worker.
func (x *IPC) settleReady(err *WorkerError) bool {
	x.mu.Lock()

	if x.readySettled {
		x.mu.Unlock()

		return false
	}

	x.readySettled = true

	if err != nil {
		x.readyErr = err
	}

	x.notify()

	x.mu.Unlock()

	if err != nil {
		go x.close()
	}

	return true
}

func (x *IPC) killOnce() {
	x.mu.Lock()

	killed := x.killed
	x.killed = true

	x.mu.Unlock()

	if !killed {
		x.kill()
	}
}

// The grace period starts with the first call, and later calls wait for the
// same deadline. A worker whose transport has ended is past asking. The
// terminate frame is sent in the background, so that a write blocked on a
// worker that has stopped reading can't hold off the kill.
func (x *IPC) close() {
	x.mu.Lock()

	if !x.terminating {
		if x.ended {
			x.waitWhile(nil, func() bool { return !x.closed })
			x.mu.Unlock()

			return
		}

		x.terminating = true
		x.deadline = time.Now().Add(x.grace)
		x.notify()

		go x.send(encodeControl(control{Type: "terminate"}))
	}

	ctx, cancel := context.WithDeadline(context.Background(), x.deadline)
	defer cancel()

	closed := x.waitWhile(ctx.Done(), func() bool { return !x.closed })

	x.mu.Unlock()

	if closed {
		return
	}

	x.killOnce()

	x.mu.Lock()
	x.waitWhile(nil, func() bool { return !x.closed })
	x.mu.Unlock()
}

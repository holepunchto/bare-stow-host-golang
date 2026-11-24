package stow_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	stow "github.com/holepunchto/bare-stow-host-golang"
)

// A worker speaks the protocol over the far end of a pipe, reading every
// frame that the host sends so that writes to the pipe never block.
type worker struct {
	t      *testing.T
	conn   net.Conn
	frames chan wire
}

type wire struct {
	tag     byte
	payload []byte
}

func newWorker(t *testing.T, conn net.Conn) *worker {
	w := &worker{t: t, conn: conn, frames: make(chan wire, 64)}

	go func() {
		defer close(w.frames)

		for {
			var header [5]byte

			if _, err := io.ReadFull(conn, header[:]); err != nil {
				return
			}

			payload := make([]byte, binary.LittleEndian.Uint32(header[:4])-1)

			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}

			w.frames <- wire{header[4], payload}
		}
	}()

	t.Cleanup(w.close)

	return w
}

func frameOf(tag byte, payload []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)+1))

	return append(append(b, tag), payload...)
}

func (w *worker) sendJSON(message string) {
	w.conn.Write(frameOf(0x00, []byte(message)))
}

func (w *worker) send(kind string) {
	w.sendJSON(`{"type":"` + kind + `"}`)
}

func (w *worker) write(data []byte) {
	w.conn.Write(frameOf(0x01, data))
}

func (w *worker) close() {
	w.conn.Close()
}

// receive returns the first control frame of kind, skipping others.
func (w *worker) receive(kind string) map[string]any {
	w.t.Helper()

	for f := range w.frames {
		var m map[string]any

		if f.tag == 0x00 && json.Unmarshal(f.payload, &m) == nil && m["type"] == kind {
			return m
		}
	}

	w.t.Fatalf("worker did not receive %s", kind)

	return nil
}

// data returns the payload of the next user data frame.
func (w *worker) data() []byte {
	w.t.Helper()

	for f := range w.frames {
		if f.tag == 0x01 {
			return f.payload
		}
	}

	w.t.Fatal("worker did not receive data")

	return nil
}

// closeOnTerminate closes the worker once the host asks it to terminate.
func (w *worker) closeOnTerminate() <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		for f := range w.frames {
			if f.tag == 0x00 && string(f.payload) == `{"type":"terminate"}` {
				break
			}
		}

		w.close()
	}()

	return done
}

// A transport whose reads fail with err and whose writes succeed.
type broken struct {
	err error
}

func (b broken) Read([]byte) (int, error) { return 0, b.err }

func (b broken) Write(p []byte) (int, error) { return len(p), nil }

func (b broken) Close() error { return nil }

// A transport whose writes fail with an error once it is set.
type unwritable struct {
	net.Conn

	mu  sync.Mutex
	err error
}

func (u *unwritable) fail(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.err = err
}

func (u *unwritable) Write(p []byte) (int, error) {
	u.mu.Lock()
	err := u.err
	u.mu.Unlock()

	if err != nil {
		return 0, err
	}

	return u.Conn.Write(p)
}

func asWorkerError(t *testing.T, err error) *stow.WorkerError {
	t.Helper()

	var w *stow.WorkerError

	if !errors.As(err, &w) {
		t.Fatalf("expected a *WorkerError, got %v", err)
	}

	return w
}

func readAll(ipc *stow.IPC) ([]byte, error) {
	return io.ReadAll(ipc)
}

func readError(t *testing.T, ipc *stow.IPC) *stow.WorkerError {
	t.Helper()

	_, err := readAll(ipc)

	return asWorkerError(t, err)
}

func writeError(t *testing.T, ipc *stow.IPC) *stow.WorkerError {
	t.Helper()

	_, err := ipc.Write([]byte("payload"))

	return asWorkerError(t, err)
}

// settle gives the host time to see the end of the transport.
func settle() {
	time.Sleep(50 * time.Millisecond)
}

// within fails the test if f doesn't return within a second.
func within[T any](t *testing.T, f func() T) T {
	t.Helper()

	done := make(chan T, 1)

	go func() { done <- f() }()

	select {
	case v := <-done:
		return v
	case <-time.After(time.Second):
		t.Fatal("timed out")

		var zero T

		return zero
	}
}

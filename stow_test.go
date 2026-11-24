package stow_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	stow "github.com/holepunchto/bare-stow-host-golang"
)

func ready(t *testing.T, ipc *stow.IPC) error {
	t.Helper()

	return within(t, func() error { return ipc.Ready(context.Background()) })
}

func TestReadyWhenTheWorkerSignals(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}
}

func TestSendsTheGraceToTheWorker(t *testing.T) {
	left, right := net.Pipe()
	stow.Wrap(left, &stow.Options{Grace: 2500 * time.Millisecond})
	w := newWorker(t, right)

	if grace := w.receive("configure")["grace"]; grace != 2500.0 {
		t.Fatalf("grace is %v", grace)
	}
}

func TestReadyFailsWhenTheWorkerErrors(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.sendJSON(`{"type":"error","message":"boom","stack":"at core.js"}`)

	closing := w.closeOnTerminate()

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerError || err.Message != "boom" || err.Stack != "at core.js" {
		t.Fatalf("unexpected error %+v", err)
	}

	<-closing
}

func TestReadyFailsWhenTheWorkerExits(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.sendJSON(`{"type":"exit","code":1}`)

	closing := w.closeOnTerminate()

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerExited || err.ExitCode != 1 {
		t.Fatalf("unexpected error %+v", err)
	}

	<-closing
}

func TestReadyFailsWhenTheTransportClosesWhileTerminating(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	closing := newWorker(t, right).closeOnTerminate()

	go ipc.Close()

	<-closing

	if err := asWorkerError(t, ready(t, ipc)); err.Code != stow.CodeWorkerClosed {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestReadyFailsWhenTheTransportFails(t *testing.T) {
	failure := errors.New("broken")
	ipc := stow.Wrap(broken{failure}, &stow.Options{Grace: 10 * time.Millisecond})

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeTransportError || err.Message != "broken" || !errors.Is(err, failure) {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestReadyFailsWhenWritingToTheTransportFails(t *testing.T) {
	failure := errors.New("broken")
	left, right := net.Pipe()
	transport := &unwritable{Conn: left}

	transport.fail(failure)

	ipc := stow.Wrap(transport, &stow.Options{Grace: 10 * time.Millisecond})
	newWorker(t, right)

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeTransportError || !errors.Is(err, failure) {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestReadyTerminatesTheWorkerWhenTheContextIsDone(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	closing := newWorker(t, right).closeOnTerminate()

	ctx, cancel := context.WithCancel(context.Background())

	cancel()

	if err := ipc.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error %v", err)
	}

	<-closing
}

func TestIgnoresExitAfterReady(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	w.sendJSON(`{"type":"exit","code":1}`)
	w.close()
	within(t, ipc.Wait)

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}
}

func TestReportsTheExitCode(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")
	w.sendJSON(`{"type":"exit","code":7}`)
	w.close()

	if code := within(t, ipc.Wait); code != 7 {
		t.Fatalf("code is %d", code)
	}

	if code := ipc.ExitCode(); code != 7 {
		t.Fatalf("code is %d", code)
	}
}

func TestCarriesUserData(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	go w.write([]byte("payload"))

	received := make([]byte, 7)

	if _, err := ipc.Read(received); err != nil || string(received) != "payload" {
		t.Fatalf("read %q, %v", received, err)
	}

	if _, err := ipc.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}

	if data := w.data(); string(data) != "reply" {
		t.Fatalf("worker received %q", data)
	}

	w.sendJSON(`{"type":"exit","code":0}`)
	w.close()

	if data, err := readAll(ipc); err != nil || len(data) != 0 {
		t.Fatalf("read %q, %v", data, err)
	}
}

func TestReadsUserDataInParts(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	go func() {
		w.write([]byte("hello"))
		w.write(nil)
		w.write([]byte(" world"))
		w.sendJSON(`{"type":"exit","code":0}`)
		w.close()
	}()

	part := make([]byte, 3)

	if n, err := ipc.Read(part); err != nil || n != 3 || string(part) != "hel" {
		t.Fatalf("read %q, %v", part[:n], err)
	}

	rest, err := readAll(ipc)

	if err != nil || !bytes.Equal(rest, []byte("lo world")) {
		t.Fatalf("read %q, %v", rest, err)
	}
}

func TestTerminateReturnsTheExitCode(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	go func() {
		w.receive("terminate")
		w.sendJSON(`{"type":"exit","code":0}`)
		w.close()
	}()

	if code := within(t, ipc.Terminate); code != 0 {
		t.Fatalf("code is %d", code)
	}
}

func TestCloseSendsTerminateToTheWorker(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	closing := newWorker(t, right).closeOnTerminate()

	within(t, ipc.Close)

	<-closing

	if code := within(t, ipc.Wait); code != -1 {
		t.Fatalf("code is %d", code)
	}
}

func TestReadyFailsAndTerminatesTheWorkerOnTimeout(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Timeout: 10 * time.Millisecond})
	closing := newWorker(t, right).closeOnTerminate()

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerTimeout {
		t.Fatalf("unexpected error %+v", err)
	}

	<-closing
}

func TestTerminateClosesTheTransportAfterTheGracePeriod(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	if code := within(t, ipc.Terminate); code != -1 {
		t.Fatalf("code is %d", code)
	}
}

func TestTerminateCallsKillAfterTheGracePeriod(t *testing.T) {
	left, right := net.Pipe()

	var killed atomic.Bool

	ipc := stow.Wrap(left, &stow.Options{
		Grace: 10 * time.Millisecond,
		Kill: func() {
			killed.Store(true)
			right.Close()
		},
	})
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	if code := within(t, ipc.Terminate); code != -1 {
		t.Fatalf("code is %d", code)
	}

	if !killed.Load() {
		t.Fatal("kill was not called")
	}
}

func TestTerminateReturnsOnceTheWorkerHasEndedAndExited(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)

	right.Close()
	settle()

	ipc.Exited(1, 0)

	if code := within(t, ipc.Terminate); code != -1 {
		t.Fatalf("code is %d", code)
	}
}

func TestExitedBeforeReadyFailsReadyWithTheExitCode(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Grace: 10 * time.Millisecond})
	newWorker(t, right)

	ipc.Exited(3, 0)

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerExited || err.ExitCode != 3 || err.Signal != 0 {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestExitedAfterReadyFailsReadsWithTheCodeAndSignal(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	ipc.Exited(137, syscall.SIGKILL)

	err := within(t, func() *stow.WorkerError { return readError(t, ipc) })

	if err.Code != stow.CodeWorkerKilled || err.ExitCode != 137 || err.Signal != syscall.SIGKILL {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestExitedIsIgnoredAfterAnExitFrame(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	w.sendJSON(`{"type":"exit","code":0}`)
	w.close()
	within(t, ipc.Wait)

	ipc.Exited(0, 0)

	if data, err := readAll(ipc); err != nil || len(data) != 0 {
		t.Fatalf("read %q, %v", data, err)
	}
}

func TestExitedIsIgnoredWhileTerminating(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	terminated := make(chan int, 1)

	go func() { terminated <- ipc.Terminate() }()

	w.receive("terminate")

	ipc.Exited(-1, syscall.SIGKILL)

	if code := <-terminated; code != -1 {
		t.Fatalf("code is %d", code)
	}

	if data, err := readAll(ipc); err != nil || len(data) != 0 {
		t.Fatalf("read %q, %v", data, err)
	}
}

func TestHoldsTheEndOfACrashedWorkerUntilItsExitIsReported(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	reading := make(chan error, 1)

	go func() {
		_, err := readAll(ipc)
		reading <- err
	}()

	w.close()
	settle()

	closing := make(chan struct{})
	waiting := make(chan struct{})

	go func() {
		ipc.Close()
		close(closing)
	}()

	go func() {
		ipc.Wait()
		close(waiting)
	}()

	settle()

	select {
	case err := <-reading:
		t.Fatalf("read ended before the exit with %v", err)
	case <-closing:
		t.Fatal("close returned before the exit")
	case <-waiting:
		t.Fatal("wait returned before the exit")
	default:
	}

	ipc.Exited(-1, syscall.SIGSEGV)

	err := asWorkerError(t, within(t, func() error { return <-reading }))

	if err.Code != stow.CodeWorkerKilled || err.Signal != syscall.SIGSEGV {
		t.Fatalf("unexpected error %+v", err)
	}

	within(t, func() struct{} { return <-closing })
	within(t, func() struct{} { return <-waiting })
}

func TestHoldsTheEndOfAWorkerThatCrashesBeforeReady(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)

	right.Close()
	settle()

	ipc.Exited(1, 0)

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerExited || err.ExitCode != 1 {
		t.Fatalf("unexpected error %+v", err)
	}

	if code := within(t, ipc.Wait); code != -1 {
		t.Fatalf("code is %d", code)
	}
}

func TestFailsACrashedWorkerWithItsExitRatherThanATimeout(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, &stow.Options{Timeout: 50 * time.Millisecond})

	right.Close()
	time.Sleep(100 * time.Millisecond)

	ipc.Exited(1, 0)

	if err := asWorkerError(t, ready(t, ipc)); err.Code != stow.CodeWorkerExited {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestFailsACrashedWorkerWithItsExitRatherThanATransportFailure(t *testing.T) {
	failure := errors.New("broken")
	ipc := stow.Wrap(broken{failure}, nil)

	settle()

	ipc.Exited(-1, syscall.SIGSEGV)

	err := asWorkerError(t, ready(t, ipc))
	cause := asWorkerError(t, errors.Unwrap(err))

	if err.Code != stow.CodeWorkerKilled || cause.Code != stow.CodeTransportError || !errors.Is(err, failure) {
		t.Fatalf("unexpected error %+v caused by %+v", err, cause)
	}
}

func TestFailsAWorkerWhoseControlFrameFailsWithItsExitRatherThanATimeout(t *testing.T) {
	left, right := net.Pipe()
	transport := &unwritable{Conn: left}

	transport.fail(errors.New("broken"))

	ipc := stow.Wrap(transport, &stow.Options{Timeout: 50 * time.Millisecond})
	newWorker(t, right)

	time.Sleep(100 * time.Millisecond)

	ipc.Exited(1, 0)

	err := asWorkerError(t, ready(t, ipc))
	cause := asWorkerError(t, errors.Unwrap(err))

	if err.Code != stow.CodeWorkerExited || cause.Code != stow.CodeTransportError {
		t.Fatalf("unexpected error %+v caused by %+v", err, cause)
	}
}

func TestKillsAWorkerWhoseTransportFailsAndThatDoesNotExit(t *testing.T) {
	failure := errors.New("broken")

	var killed atomic.Bool

	ipc := stow.Wrap(broken{failure}, &stow.Options{
		Grace: 10 * time.Millisecond,
		Kill:  func() { killed.Store(true) },
	})

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeTransportError || !errors.Is(err, failure) {
		t.Fatalf("unexpected error %+v", err)
	}

	if !killed.Load() {
		t.Fatal("kill was not called")
	}

	ipc.Exited(1, 0)

	if err := readError(t, ipc); err.Code != stow.CodeTransportError {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestTreatsAResetTransportAsClosed(t *testing.T) {
	reset := &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}
	ipc := stow.Wrap(broken{reset}, nil)

	settle()

	ipc.Exited(1, 0)

	err := asWorkerError(t, ready(t, ipc))

	if err.Code != stow.CodeWorkerExited || errors.Unwrap(err) != nil {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestFailsWritesWhenTheTransportFails(t *testing.T) {
	failure := errors.New("broken")
	left, right := net.Pipe()
	transport := &unwritable{Conn: left}
	ipc := stow.Wrap(transport, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.receive("configure")
	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	transport.fail(failure)

	if err := writeError(t, ipc); err.Code != stow.CodeTransportError || !errors.Is(err, failure) {
		t.Fatalf("unexpected error %+v", err)
	}

	if err := within(t, func() *stow.WorkerError { return readError(t, ipc) }); err.Code != stow.CodeTransportError {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestEndsWhenAWriteFindsTheWorkerGone(t *testing.T) {
	left, right := net.Pipe()
	transport := &unwritable{Conn: left}
	ipc := stow.Wrap(transport, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.receive("configure")
	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	transport.fail(&os.SyscallError{Syscall: "write", Err: syscall.EPIPE})

	if err := writeError(t, ipc); err.Code != stow.CodeWorkerClosed {
		t.Fatalf("unexpected error %+v", err)
	}

	w.sendJSON(`{"type":"exit","code":3}`)
	w.close()

	if code := within(t, ipc.Wait); code != 3 {
		t.Fatalf("code is %d", code)
	}

	if data, err := readAll(ipc); err != nil || len(data) != 0 {
		t.Fatalf("read %q, %v", data, err)
	}
}

func TestFailsWritesAfterTheEndAsClosed(t *testing.T) {
	left, right := net.Pipe()
	transport := &unwritable{Conn: left}
	ipc := stow.Wrap(transport, &stow.Options{Grace: 10 * time.Millisecond})
	w := newWorker(t, right)

	w.receive("configure")
	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	w.close()
	settle()

	transport.fail(errors.New("broken"))

	if err := writeError(t, ipc); err.Code != stow.CodeWorkerClosed {
		t.Fatalf("unexpected error %+v", err)
	}

	ipc.Exited(1, 0)

	if err := readError(t, ipc); err.Code != stow.CodeWorkerExited || errors.Unwrap(err) != nil {
		t.Fatalf("unexpected error %+v", err)
	}
}

func TestEndsAWorkerThatExitsOnItsOwnWithoutHolding(t *testing.T) {
	left, right := net.Pipe()
	ipc := stow.Wrap(left, nil)
	w := newWorker(t, right)

	w.send("ready")

	if err := ready(t, ipc); err != nil {
		t.Fatal(err)
	}

	w.sendJSON(`{"type":"exit","code":0}`)
	w.close()

	if code := within(t, ipc.Wait); code != 0 {
		t.Fatalf("code is %d", code)
	}
}

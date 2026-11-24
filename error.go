package stow

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
)

// The codes of a WorkerError.
const (
	// CodeWorkerError is the code of a fault that the worker reported.
	CodeWorkerError = "WORKER_ERROR"
	// CodeWorkerExited is the code of a worker that exited without saying why.
	CodeWorkerExited = "WORKER_EXITED"
	// CodeWorkerKilled is the code of a worker that was killed by a signal.
	CodeWorkerKilled = "WORKER_KILLED"
	// CodeWorkerClosed is the code of a transport that closed before ready
	// once the worker was asked to terminate, or of a write after it ended.
	CodeWorkerClosed = "WORKER_CLOSED"
	// CodeWorkerTimeout is the code of a worker that timed out before ready.
	CodeWorkerTimeout = "WORKER_TIMEOUT"
	// CodeTransportError is the code of a transport that failed while reading
	// or writing.
	CodeTransportError = "TRANSPORT_ERROR"
)

// A WorkerError is a fault reported by or observed in the worker.
type WorkerError struct {
	// Code is the kind of the fault, one of the Code constants.
	Code string
	// Message is the message of the error.
	Message string
	// Stack is the stack of the error reported by the worker, if any.
	Stack string
	// ExitCode is the exit code of a worker that exited without saying why,
	// or -1 if there is none.
	ExitCode int
	// Signal is the signal that killed a worker that exited without saying
	// why, or 0 if there is none.
	Signal syscall.Signal

	err error
}

func newError(code, message string) *WorkerError {
	return &WorkerError{Code: code, Message: message, ExitCode: -1}
}

func transportError(err error) *WorkerError {
	e := newError(CodeTransportError, err.Error())
	e.err = err

	return e
}

func closedError() *WorkerError {
	return newError(CodeWorkerClosed, "Worker closed")
}

func (e *WorkerError) Error() string {
	return e.Message
}

// Unwrap returns the error that caused this one: The transport failure of a
// TRANSPORT_ERROR, or the TRANSPORT_ERROR that preceded the exit of a worker.
func (e *WorkerError) Unwrap() error {
	return e.err
}

// A peer that goes away resets or breaks the connection rather than ending it,
// which is no different from it closing, as is a transport closed by kill.
func isEnd(err error) bool {
	for _, end := range endErrors {
		if errors.Is(err, end) {
			return true
		}
	}

	return false
}

var endErrors = append([]error{
	syscall.EPIPE,
	syscall.ECONNRESET,
	syscall.ECONNABORTED,
	io.ErrClosedPipe,
	net.ErrClosed,
	os.ErrClosed,
}, platformEndErrors...)

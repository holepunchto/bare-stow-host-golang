package stow

import "syscall"

// The syscall package lacks ERROR_NO_DATA, which a write to a pipe whose
// reader has closed fails with.
const errorNoData = syscall.Errno(232)

var platformEndErrors = []error{
	syscall.ERROR_BROKEN_PIPE,
	errorNoData,
	syscall.WSAECONNRESET,
	syscall.WSAECONNABORTED,
}

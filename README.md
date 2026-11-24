# bare-stow-host-golang

Host side of the [bare-stow](https://github.com/holepunchto/bare-stow) lifecycle for Go. The Go counterpart of [bare-stow-host](https://github.com/holepunchto/bare-stow-host): it multiplexes a control channel and a user data channel over a byte transport to a stowed bundle, and layers readiness, termination, and exit reporting on top.

```
go get github.com/holepunchto/bare-stow-host-golang
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"os/exec"
	"syscall"

	sidecar "github.com/holepunchto/bare-sidecar-golang"
	stow "github.com/holepunchto/bare-stow-host-golang"
)

func main() {
	cmd := exec.Command("path/to/bare", "index.bundle")

	s, err := sidecar.Start(cmd)
	if err != nil {
		panic(err)
	}

	ipc := stow.Wrap(s, &stow.Options{
		Kill: func() { cmd.Process.Kill() },
	})

	go func() {
		cmd.Wait()

		status := cmd.ProcessState.Sys().(syscall.WaitStatus)

		ipc.Exited(cmd.ProcessState.ExitCode(), status.Signal())
	}()

	if err := ipc.Ready(context.Background()); err != nil {
		panic(err)
	}

	ipc.Write([]byte("hello"))

	fmt.Println("exited with", ipc.Terminate())
}
```

Any `io.ReadWriteCloser` can be wrapped, such as a `Sidecar` from [bare-sidecar-golang](https://github.com/holepunchto/bare-sidecar-golang). Reading from the handle yields the user data from the worker, and writing to it sends user data to the worker. Use `stow.Options` to set `Kill` to stop a worker that ignores `terminate`, which closes the transport by default, the `Timeout` to wait for the worker to signal ready, and the `Grace` period a terminating worker gets before it's killed.

Whoever wraps a transport must report the exit of the worker with `IPC.Exited()`, as the example does once the process has exited. The end of a worker that crashes is held back until its exit is reported, so that reads and `IPC.Ready()` fail with the exit code or signal instead of ending cleanly. A transport that fails instead waits for the exit for the grace period, and the worker is killed if it hasn't exited by then. A transport that is reset or broken by the worker going away counts as ending rather than failing.

The errors are `*stow.WorkerError` values, whose `Code` is one of `WORKER_ERROR`, `WORKER_EXITED`, `WORKER_KILLED`, `WORKER_CLOSED`, `WORKER_TIMEOUT`, and `TRANSPORT_ERROR`. Use `errors.As` to get one and `errors.Unwrap` to get the failure that caused it.

## Protocol

Every frame is a 4-byte little-endian length followed by that many bytes: A 1-byte tag and the payload. The tag is `0x00` for a control frame, whose payload is a UTF-8 JSON object carrying its `type`, and `0x01` for a user data frame. See [bare-stow-protocol](https://github.com/holepunchto/bare-stow-protocol) for the control frames.

## License

Apache-2.0

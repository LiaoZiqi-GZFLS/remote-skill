# Embedded rskilld binaries

`rsk` embeds the remote daemon binaries found in this directory so it can
upload the right build to a server without a toolchain on either side.

Populate it with:

```
go run ./tools/pack
```

which produces `rskilld_<goos>_<goarch>` files here. When no build matches the
remote platform, `rsk` falls back to a local `go build` cross compile.

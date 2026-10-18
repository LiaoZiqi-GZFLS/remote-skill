# GNU make 3.82+ (.RECIPEPREFIX keeps recipes readable without tabs)
.RECIPEPREFIX = >
GO ?= go

pack:
> $(GO) run ./tools/pack

build: pack
> $(GO) build -trimpath -o dist/rsk ./cmd/rsk
> $(GO) build -trimpath -o dist/rskilld ./cmd/rskilld

test:
> $(GO) test ./...

vet:
> $(GO) vet ./...

fmt:
> $(GO) fmt ./...

e2e:
> pwsh -NoProfile -File test/e2e.ps1 -Target $(TARGET) -User $(USER)

clean:
> rm -rf dist internal/assets/bin/rskilld_*

.PHONY: pack build test vet fmt e2e clean

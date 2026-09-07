set shell := ["bash", "-euo", "pipefail", "-c"]
export GOWORK := "off"
demo := "cmd/badgerbox-demo"
lint_version := "2.12.1"

default:
    @just --list

build:
    go build ./...
    cd {{demo}} && go build ./...

test:
    go test -race ./...
    cd {{demo}} && go test -race ./...

test-integration:
    go test -race -tags=integration ./...
    cd {{demo}} && go test -race -tags=integration ./...

lint: lint-install
    ./bin/golangci-lint run --build-tags=integration ./...
    cd {{demo}} && ../../bin/golangci-lint run --build-tags=integration ./...

lint-install:
    @if ! test -x bin/golangci-lint || ! bin/golangci-lint version | grep -q '{{lint_version}}'; then mkdir -p bin; GOBIN="$(pwd)/bin" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v{{lint_version}}; fi

format:
    git ls-files -z '*.go' | xargs -0 gofmt -w

format-check:
    @files=$(git ls-files '*.go'); result=$(gofmt -l $files); if test -n "$result"; then printf '%s\n' "$result"; exit 1; fi

coverage:
    mkdir -p .artifacts
    go test -race -coverprofile=.artifacts/coverage.out ./...
    cd {{demo}} && go test -race -coverprofile=../../.artifacts/demo-coverage.out ./...

benchmark:
    go test -run='^$' -bench=. -benchmem ./...
    cd {{demo}} && go test -run='^$' -bench=. -benchmem ./...

# Finite disk-backed demo verification; pass additional flags after the recipe.
benchmark-demo *args:
    cd {{demo}} && go run . benchmark {{args}}

# Repeated sequential runs; output must name a new directory.
benchmark-matrix output:
    mkdir -p bin
    cd {{demo}} && go build -o ../../bin/badgerbox-demo .
    python3 scripts/benchmark/matrix.py --binary bin/badgerbox-demo --output-dir {{output}}

check: format-check build lint test

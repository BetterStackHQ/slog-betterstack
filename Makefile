.PHONY: build test vet fmt lint coverage

build:
	go build ./...

test:
	go test -race -count=1 ./...

vet:
	go vet ./...

# Fails when any file is not gofmt-formatted.
fmt:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi

lint:
	golangci-lint run ./...

coverage:
	go test -race -count=1 -coverprofile=cover.out -covermode=atomic ./...
	go tool cover -html=cover.out -o cover.html

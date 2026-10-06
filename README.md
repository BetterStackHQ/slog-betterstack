# [Better Stack](https://betterstack.com/logs) Go client

[![Better Stack dashboard](https://github.com/logtail/logtail-python/assets/10132717/e2a1196b-7924-4abc-9b85-055e17b5d499)](https://betterstack.com/logs)

[![MIT License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/BetterStackHQ/slog-betterstack.svg)](https://pkg.go.dev/github.com/BetterStackHQ/slog-betterstack)
[![build](https://github.com/BetterStackHQ/slog-betterstack/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/BetterStackHQ/slog-betterstack/actions/workflows/main.yml)

Experience SQL-compatible structured log management based on ClickHouse. [Learn more ⇗](https://betterstack.com/logs)

A [`log/slog`](https://pkg.go.dev/log/slog) handler that sends the logs of your Go application to Better Stack.

## Documentation

[Getting started ⇗](https://betterstack.com/docs/logs/go/)

## Install

```sh
go get github.com/BetterStackHQ/slog-betterstack
```

Go 1.21 or newer.

## Usage

```go
package main

import (
	"log/slog"

	slogbetterstack "github.com/BetterStackHQ/slog-betterstack"
)

func main() {
	logger := slog.New(
		slogbetterstack.Option{
			Token:    "$SOURCE_TOKEN",
			Endpoint: "https://$INGESTING_HOST/",
		}.NewBetterstackHandler(),
	)

	logger.Info("Hello from Better Stack!", "service", "UserService")
}
```

Find the source token and the ingesting host of your source in Better Stack → Sources.

Logs are sent asynchronously. For short-lived programs, such as CLI tools or one-off scripts, give
the handler a moment to finish sending before the process exits, for example with
`time.Sleep(5 * time.Second)` after the last log line. Long-running services are not affected.

### Options

```go
type Option struct {
	// log level (default: debug)
	Level slog.Leveler

	// token
	Token string
	// optional: endpoint
	Endpoint string
	// default: 10s
	Timeout time.Duration

	// optional: customize record builder
	Converter Converter
	// optional: custom marshaler
	Marshaler func(v any) ([]byte, error)
	// optional: fetch attributes from context
	AttrFromContext []func(ctx context.Context) []slog.Attr

	// optional: see slog.HandlerOptions
	AddSource   bool
	ReplaceAttr func(groups []string, a slog.Attr) slog.Attr
}
```

Package-level settings:

```go
slogbetterstack.SourceKey = "runtime"              // where AddSource puts the call site
slogbetterstack.ContextKey = "extra"               // where attributes are nested
slogbetterstack.ErrorKeys = []string{"error", "err"} // attributes expanded as errors
```

### Structured data

```go
logger := slog.New(slogbetterstack.Option{Level: slog.LevelDebug, Token: "$SOURCE_TOKEN"}.NewBetterstackHandler())
logger = logger.With("release", "v1.0.0")

logger.
	With(
		slog.Group("user",
			slog.String("id", "user-123"),
			slog.Time("created_at", time.Now()),
		),
	).
	With("error", fmt.Errorf("an error")).
	Error("a message", slog.Int("count", 1))
```

The record above arrives in Better Stack as:

```json
{
  "dt": "2026-10-06T09:50:54.721381Z",
  "level": "ERROR",
  "message": "a message",
  "logger.name": "BetterStackHQ/slog-betterstack",
  "logger.version": "1.4.4",
  "extra": {
    "release": "v1.0.0",
    "user": {"id": "user-123", "created_at": "2026-10-06T11:50:54.721344+02:00"},
    "error": {"kind": "*errors.errorString", "error": "an error", "stack": null},
    "count": 1
  }
}
```

### Tracing

Attach trace and span ids from the context with [samber/slog-otel](https://github.com/samber/slog-otel):

```go
import (
	slogbetterstack "github.com/BetterStackHQ/slog-betterstack"
	slogotel "github.com/samber/slog-otel"
)

logger := slog.New(
	slogbetterstack.Option{
		Token: "$SOURCE_TOKEN",
		AttrFromContext: []func(ctx context.Context) []slog.Attr{
			slogotel.ExtractOtelAttrFromContext([]string{"tracing"}, "trace_id", "span_id"),
		},
	}.NewBetterstackHandler(),
)

logger.ErrorContext(ctx, "a message")
```

To log to Better Stack and somewhere else at the same time, combine handlers with
[samber/slog-multi](https://github.com/samber/slog-multi) or, from Go 1.26, `slog.NewMultiHandler`.

## Upgrading from samber/slog-betterstack

This repository continues [samber/slog-betterstack](https://github.com/samber/slog-betterstack),
which Better Stack maintains as its official Go client from version 1.5.0 on. The API and the
shape of the logs are unchanged, so upgrading means replacing the import path:

```sh
go get github.com/BetterStackHQ/slog-betterstack@latest
go mod edit -droprequire github.com/samber/slog-betterstack
```

```go
import slogbetterstack "github.com/BetterStackHQ/slog-betterstack"
```

## Need help?

Please let us know at [hello@betterstack.com](mailto:hello@betterstack.com). We're happy to help!

## Credits

slog-betterstack was created and maintained by [Samuel Berthe](https://github.com/samber) as part
of his [slog ecosystem](https://github.com/samber?tab=repositories&q=slog) and released under the
MIT license. Thank you, Samuel! ❤️

---

[MIT license](LICENSE), [example project](example-project/)

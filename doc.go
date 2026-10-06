// Package slogbetterstack sends the logs of a Go application to Better Stack through a
// [log/slog] handler.
//
//	handler := slogbetterstack.Option{
//		Token:    "$SOURCE_TOKEN",
//		Endpoint: "https://$INGESTING_HOST/",
//		Level:    slog.LevelInfo, // Debug if omitted
//	}.NewBetterstackHandler()
//	defer handler.Close()
//
//	logger := slog.New(handler)
//	logger.Info("Hello from Better Stack!", "service", "UserService")
//
// Records are queued and uploaded in batches by a background goroutine, so logging never
// waits for the network. Close delivers what is still queued and must run before the program
// exits; os.Exit and log.Fatal skip deferred calls. Delivery failures, and a missing token,
// are reported through [Option.OnError], on stderr by default, and counted in [Stats].
//
// See https://betterstack.com/docs/logs/go/ for the full documentation.
package slogbetterstack

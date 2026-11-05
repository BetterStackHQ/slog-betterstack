// A small program that sends a few log records to Better Stack.
//
// Run it with the source token and the ingesting host of your source, both shown in
// Better Stack → Sources:
//
//	BETTER_STACK_SOURCE_TOKEN=... BETTER_STACK_INGESTING_HOST=... go run ./example-project
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	slogbetterstack "github.com/BetterStackHQ/slog-betterstack"
)

func main() {
	token := os.Getenv("BETTER_STACK_SOURCE_TOKEN")
	host := os.Getenv("BETTER_STACK_INGESTING_HOST")
	if token == "" || host == "" {
		fmt.Fprintln(os.Stderr, "usage: BETTER_STACK_SOURCE_TOKEN=<source token> BETTER_STACK_INGESTING_HOST=<ingesting host> go run ./example-project")
		os.Exit(1)
	}

	handler := slogbetterstack.Option{
		Token:    token,
		Endpoint: "https://" + host + "/",
		Level:    slog.LevelInfo, // Debug if omitted
	}.NewBetterstackHandler()
	logger := slog.New(handler).With("release", "v1.0.0")

	logger.Info("Starting user service.", "service", "UserService")

	// Below the handler's Level, so it is not sent.
	logger.Debug("Loading the user cache.")

	logger.Warn("User profile is incomplete.",
		slog.Group("user",
			slog.String("id", "user-123"),
			slog.Time("created_at", time.Date(2024, time.March, 1, 12, 0, 0, 0, time.UTC)),
		),
		slog.Any("missing", []string{"email", "phone"}),
	)

	err := fmt.Errorf("fetching user user-123: %w", context.DeadlineExceeded)
	logger.With("userID", 123).Error("Unable to fetch user data.", "error", err)

	// Records are sent in batches: Close delivers what is still queued before the program exits.
	if err := handler.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(1)
	}

	// A record Better Stack rejected is reported on stderr when it happens and counted in Stats.
	stats := handler.Stats()
	if stats.Sent != stats.Enqueued {
		fmt.Fprintf(os.Stderr, "Only %d of %d log records were delivered.\n", stats.Sent, stats.Enqueued)
		os.Exit(1)
	}
	fmt.Printf("Sent %d log records. Open Better Stack → Live tail to see them.\n", stats.Sent)
}

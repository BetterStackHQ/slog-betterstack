// A small program that sends a few log records to Better Stack.
//
// Run it with the source token and the ingesting host of your source, both shown in
// Better Stack → Sources:
//
//	BETTERSTACK_SOURCE_TOKEN=... BETTERSTACK_INGESTING_HOST=... go run ./example-project
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	slogbetterstack "github.com/BetterStackHQ/slog-betterstack"
)

func main() {
	token := os.Getenv("BETTERSTACK_SOURCE_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "set BETTERSTACK_SOURCE_TOKEN to the source token from Better Stack → Sources")
		os.Exit(1)
	}

	option := slogbetterstack.Option{Level: slog.LevelDebug, Token: token}
	if host := os.Getenv("BETTERSTACK_INGESTING_HOST"); host != "" {
		option.Endpoint = "https://" + host + "/"
	}

	logger := slog.New(option.NewBetterstackHandler())
	logger = logger.With("release", "v1.0.0")

	logger.Debug("Debugging user service.", "service", "UserService")

	logger.With("userID", 123).Error("Unable to fetch user data.")

	logger.
		With(
			slog.Group("user",
				slog.String("id", "user-123"),
				slog.Time("created_at", time.Now()),
			),
		).
		With("error", fmt.Errorf("an error")).
		Error("a message", slog.Int("count", 1))

	// Logs are sent asynchronously: give the handler a moment before the process exits.
	time.Sleep(5 * time.Second)

	fmt.Println("Sent 3 log records. Open Better Stack → Live tail to see them.")
}

package log

import (
	"context"
	"log/slog"
)

// Unexported new type so that our context key never collides with another.
type contextKeyType struct{}

// contextKey is the key used for the context to store the logger.
var contextKey = contextKeyType{}

func WithContext(ctx context.Context, logger *slog.Logger, args ...interface{}) context.Context {
	// While we could call logger.With even with zero args, we have this
	// check to avoid unnecessary allocations around creating a copy of a
	// logger.
	if len(args) > 0 {
		logger = logger.With(args...)
	}

	return context.WithValue(ctx, contextKey, logger)
}

func FromContext(ctx context.Context) *slog.Logger {
	logger, _ := ctx.Value(contextKey).(*slog.Logger)
	if logger == nil {
		return slog.Default()
	}

	return logger
}

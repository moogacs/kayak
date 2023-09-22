package log

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/fatih/color"
	"go.opentelemetry.io/otel/trace"
)

type TextHandlerOptions struct {
	SlogOpts *slog.HandlerOptions
}
type TextHandler struct {
	slog.Handler
	l *log.Logger
}

func NewTextHandler(
	out io.Writer,
	opts *slog.HandlerOptions,
) *TextHandler {
	h := &TextHandler{
		Handler: slog.NewJSONHandler(out, opts),
		l:       log.New(out, "", 0),
	}

	return h
}
func (h *TextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TextHandler{
		l:       h.l,
		Handler: h.Handler.WithAttrs(attrs),
	}
}
func (h *TextHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Handler.Enabled(ctx, r.Level) {
		return nil
	}
	scontext := trace.SpanContextFromContext(ctx)
	if scontext.HasTraceID() {
		r.AddAttrs(slog.Any("trace_id", scontext.TraceID()))
	}
	level := "[" + r.Level.String() + "]"
	switch r.Level {
	case slog.LevelDebug:
		level = color.MagentaString(level)
	case slog.LevelInfo:
		level = color.GreenString(level)
	case slog.LevelWarn:
		level = color.YellowString(level)
	case slog.LevelError:
		level = color.RedString(level)
	}

	fields := make(map[string]interface{}, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		key := color.CyanString(a.Key)
		value := color.WhiteString("%v", a.Value.Any())
		if a.Key == "error" {
			value = color.RedString("%v", a.Value.Any())
		}
		fields[key] = value

		return true
	})
	// c := color.New(color.FgBlack).Add(color.Faint)
	s := r.Time.Format(time.RFC3339)
	timeStr := fmt.Sprintf("\x1b[%dm%v\x1b[0m", 90, s)
	//timeStr := c.Sprintf()
	msg := color.WhiteString(r.Message)
	var b strings.Builder
	for key, value := range fields {
		fmt.Fprintf(&b, "%s=%v ", key, value)
	}

	h.l.Println(timeStr, level, msg, b.String())

	return nil
}

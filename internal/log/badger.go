package log

import (
	"fmt"
	"strings"

	"log/slog"
)

type BadgerLogger struct{}

func (b *BadgerLogger) Errorf(msg string, args ...interface{}) {
	line := strings.TrimSuffix(fmt.Sprintf(msg, args...), "\n")
	slog.Error(line, "component", "badger")
}
func (b *BadgerLogger) Warningf(msg string, args ...interface{}) {
	line := strings.TrimSuffix(fmt.Sprintf(msg, args...), "\n")
	slog.Warn(line, "component", "badger")
}
func (b *BadgerLogger) Infof(msg string, args ...interface{}) {
	line := strings.TrimSuffix(fmt.Sprintf(msg, args...), "\n")
	slog.Info(line, "component", "badger")
}
func (b *BadgerLogger) Debugf(msg string, args ...interface{}) {
	line := strings.TrimSuffix(fmt.Sprintf(msg, args...), "\n")
	slog.Debug(line, "component", "badger")
}

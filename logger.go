package workerpool

import "log"

// Logger is a minimal logging interface.
type Logger interface {
	Printf(format string, args ...any)
}

type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

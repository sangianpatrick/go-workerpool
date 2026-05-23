package workerpool

import "log"

// Logger is a minimal logging interface.
type Logger interface {
	Printf(format string, args ...any)
}

// defaultLogger is the default implementation of Logger using the standard log package.
type defaultLogger struct{}

// Printf implements the Logger interface using the standard log package.
func (l *defaultLogger) Printf(format string, args ...any) {
	log.Printf(format, args...)
}

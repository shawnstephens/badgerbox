package demo

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
)

type ColorMode string

const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

type Logger struct {
	out      io.Writer
	useColor bool
	mu       sync.Mutex
	phase    map[string]*color.Color
}

func NewLogger(out io.Writer, mode string) *Logger {
	useColor := detectColor(mode)
	return &Logger{
		out:      out,
		useColor: useColor,
		phase: map[string]*color.Color{
			"startup":     color.New(color.FgCyan),
			"ready":       color.New(color.FgGreen),
			"enqueue":     color.New(color.FgBlue),
			"process":     color.New(color.FgMagenta),
			"publish":     color.New(color.FgHiGreen),
			"consume":     color.New(color.FgYellow),
			"maintenance": color.New(color.FgHiBlue),
			"warning":     color.New(color.FgHiYellow),
			"shutdown":    color.New(color.FgHiCyan),
			"error":       color.New(color.FgRed),
		},
	}
}

func detectColor(mode string) bool {
	switch ColorMode(strings.ToLower(mode)) {
	case ColorAlways:
		return true
	case ColorNever:
		return false
	default:
		return !color.NoColor && isTerminal()
	}
}

func isTerminal() bool {
	info, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func (l *Logger) Printf(phase, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format(time.RFC3339Nano)
	line := fmt.Sprintf("%s phase=%s %s\n", timestamp, phase, fmt.Sprintf(format, args...))
	if !l.useColor {
		_, _ = io.WriteString(l.out, line)
		return
	}

	styler := l.phase[phase]
	if styler == nil {
		styler = color.New()
	}
	_, _ = io.WriteString(l.out, styler.Sprint(line))
}

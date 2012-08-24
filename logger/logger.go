package logger

import (
	"fmt"
	"log"
	"os"
)

var stdout = log.New(os.Stdout, "", log.LstdFlags)
var stderr = log.New(os.Stderr, "", log.LstdFlags)

func Log(level, message string) {
	switch level {
	case "fatal":
		stderr.Fatalf("level=fatal %s\n", message)
	case "error":
		stderr.Printf("level=error %s\n", message)
	default:
		stdout.Printf("level=%s %s\n", level, message)
	}
	return
}

func LogEvent(level string, code string, param string, value string) {
	Log(level, fmt.Sprintf("event=%s %v=%q", code, param, value))
	return
}

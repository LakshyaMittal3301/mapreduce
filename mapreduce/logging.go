package mr

import (
	"log"
	"strings"
	"sync/atomic"
)

type logLevel int32

const (
	levelInfo logLevel = iota
	levelDebug
)

var currentLevel atomic.Int32
 
func init() {
	currentLevel.Store(int32(levelInfo))
}

// SetLogLevel sets logging level; accepts "info" (default) or "debug".
func SetLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		currentLevel.Store(int32(levelDebug))
	default:
		currentLevel.Store(int32(levelInfo))
	}
}

func Infof(format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func Debugf(format string, args ...interface{}) {
	if logLevel(currentLevel.Load()) >= levelDebug {
		log.Printf("[DEBUG] "+format, args...)
	}
}

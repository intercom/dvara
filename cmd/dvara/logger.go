package main

import (
	"fmt"

	corelog "github.com/intercom/gocore/log"
)

type Logger struct {
}

func (l *Logger) Debugf(f string, args ...interface{}) {
	corelog.LogInfoMessage(fmt.Sprintf(f, args...))
}

func (l *Logger) Errorf(f string, args ...interface{}) {
	corelog.LogErrorMessage(fmt.Sprintf(f, args...))
}

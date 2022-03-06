package common

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

func InitLogger(level string, appName string) (*log.Logger, error) {
	switch strings.ToLower(level) {
	case "trace": log.SetLevel(log.TraceLevel)
	case "debug": log.SetLevel(log.DebugLevel)
	case "info": log.SetLevel(log.InfoLevel)
	case "warn": log.SetLevel(log.WarnLevel)
	case "error": log.SetLevel(log.ErrorLevel)
	case "fatal": log.SetLevel(log.FatalLevel)
	case "panic": log.SetLevel(log.PanicLevel)
	default:
		return nil, fmt.Errorf("unsupported log level %s", level)
	}
	logger := log.New()
	logger.SetFormatter(&MyLogFormatter{AppName: appName})
	return logger, nil
}


type MyLogFormatter struct {
	AppName	string
}

func (f *MyLogFormatter) Format(entry *log.Entry) ([]byte, error)  {
	year, month, day := entry.Time.Date()
	hour, minute, second := entry.Time.Clock()
	str := fmt.Sprintf("%d/%02d/%02d %02d:%02d:%02d %s [%s] %s\n", year, month, day, hour, minute, second,
		strings.ToUpper(entry.Level.String()), f.AppName, entry.Message)
	return []byte(str), nil
}



package common

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

func InitLogger(level string, appName string) (*log.Logger, error) {
	logger := log.New()
	switch strings.ToLower(level) {
	case "trace": logger.SetLevel(log.TraceLevel)
	case "debug": logger.SetLevel(log.DebugLevel)
	case "info": logger.SetLevel(log.InfoLevel)
	case "warn": logger.SetLevel(log.WarnLevel)
	case "error": logger.SetLevel(log.ErrorLevel)
	case "fatal": logger.SetLevel(log.FatalLevel)
	case "panic": logger.SetLevel(log.PanicLevel)
	default:
		return nil, fmt.Errorf("unsupported log level %s", level)
	}
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



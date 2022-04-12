package common

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

func InitLogger(level, appName string) (*log.Logger, error) {
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
	// if path != "" {
	// 	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("can not open log file %s: %v", path, err)
	// 	}
	// 	writer := io.MultiWriter(os.Stdout, logFile)
	// 	logger.SetOutput(writer)
	// }
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



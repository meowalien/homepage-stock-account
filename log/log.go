package log

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func InitLogger() {
	logLevel := viper.GetString("log.level")
	switch logLevel {
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	default:
		logrus.SetLevel(logrus.InfoLevel)
	}
}

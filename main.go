package main

import (
	"github.com/sirupsen/logrus"
	"price-reporter/config"
	"price-reporter/log"
	"price-reporter/quit"
	"price-reporter/rabbitmq"
	"price-reporter/reporter"
)

func main() {
	defer logrus.Info("Main exiting")
	defer quit.WaitForAllGoroutineEnd()
	config.InitConfig()
	log.InitLogger()
	rabbitmq.InitRabbitMQ()
	defer rabbitmq.CloseRabbitMQ()

	reporterInst := reporter.NewReporter()
	reporterInst.Start()
	defer reporterInst.Close()

	quit.WaitForQuitSignal()

}

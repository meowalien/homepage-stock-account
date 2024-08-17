package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"price-reporter/config"
	"price-reporter/log"
	"price-reporter/quit"
	"price-reporter/rabbitmq"
	"price-reporter/reporter"
	"time"
)

const InitializationTimeout = 30 * time.Second
const FinalizeTimeout = time.Second * 10

func main() {
	defer logrus.Info("Main exiting")
	defer quit.WaitForAllGoroutineEnd(FinalizeTimeout)
	err := config.InitConfig()
	if err != nil {
		logrus.Fatalf("Failed to initialize config: %v", err)
	}
	log.InitLogger()

	ctx, cancel := context.WithTimeoutCause(context.Background(), InitializationTimeout, fmt.Errorf("initilization timeout"))
	defer cancel()

	err = rabbitmq.InitRabbitMQ(ctx)
	if err != nil {
		logrus.Fatalf("Failed to initialize RabbitMQ: %v", err)
	}
	defer rabbitmq.CloseRabbitMQ()

	reporterInst := reporter.NewReporter()
	err = reporterInst.Start(ctx)
	if err != nil {
		logrus.Fatalf("Failed to start reporter: %v", err)
	}
	defer reporterInst.Close()

	quit.WaitForQuitSignal()
}

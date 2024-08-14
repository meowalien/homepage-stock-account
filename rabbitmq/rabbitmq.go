package rabbitmq

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"price-reporter/schedule"
	"time"
)

var conn *amqp.Connection

func InitRabbitMQ() {
	var err error
	done := schedule.Retry(context.Background(), 3, time.Second, func(round int) bool {
		conn, err = amqp.Dial(viper.GetString("rabbitmq.url"))
		if err != nil {
			logrus.Errorf("Failed to connect to RabbitMQ[%d]: %s", round, err)
			return false
		}
		return true
	})
	if !done {
		logrus.Panic(err)
	}

	logrus.Info("Connected to RabbitMQ")
}

func NewChannel() (*amqp.Channel, error) {
	newChannel, err := conn.Channel()
	if err != nil {
		// check if is "channel/connection is not open"
		if errors.Is(err, amqp.ErrClosed) {
			logrus.Error("Connection is closed, reconnecting...")
			InitRabbitMQ()
			return NewChannel()
		}

		logrus.Errorf("Failed to create channel: %s", err)
		done := schedule.Retry(context.Background(), 3, time.Second, func(round int) bool {
			logrus.Infof("Retry to create channel [%d]", round)
			newChannel, err = conn.Channel()
			return err != nil
		})
		if !done {
			return nil, err
		}
	}
	return newChannel, nil
}

func CloseRabbitMQ() {
	if conn != nil {
		conn.Close()
	}
}

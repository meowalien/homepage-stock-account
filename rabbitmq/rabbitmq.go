package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"price-reporter/schedule"
	"sync/atomic"
	"time"
)

var conn atomic.Pointer[amqp.Connection]

func InitRabbitMQ(ctx context.Context) (err error) {
	err = schedule.Retry(ctx, -1, time.Second, func(round int) bool {
		var newConn *amqp.Connection
		newConn, err = amqp.Dial(viper.GetString("rabbitmq.url"))
		if err != nil {
			logrus.Errorf("Failed to connect to RabbitMQ[%d]: %s", round, err)
			return false
		}
		conn.Store(newConn)
		return true
	})
	fmt.Println("Retry end")
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	logrus.Info("Connected to RabbitMQ")
	return nil
}

func NewChannel(ctx context.Context) (*amqp.Channel, error) {
	currentConn := conn.Load()
	if currentConn == nil {
		logrus.Error("Connection is not initialized")
		return nil, errors.New("connection is not initialized")
	}

	newChannel, err := currentConn.Channel()
	if err != nil {
		// check if "channel/connection is not open"
		if errors.Is(err, amqp.ErrClosed) {
			logrus.Error("Connection is closed, reconnecting...")
			errInitRabbitMQ := InitRabbitMQ(ctx)
			if errInitRabbitMQ != nil {
				logrus.Errorf("Failed to reconnect to RabbitMQ: %s", errInitRabbitMQ)
				return nil, errInitRabbitMQ
			}
			return NewChannel(ctx)
		}

		logrus.Errorf("Failed to create channel: %s", err)
		err = schedule.Retry(ctx, 3, time.Second, func(round int) bool {
			logrus.Infof("Retry to create channel [%d]", round)
			newChannel, err = currentConn.Channel()
			if err != nil {
				logrus.Errorf("Fail to make channel: %s", err)
				return false
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	return newChannel, nil
}

func CloseRabbitMQ() {
	currentConn := conn.Load()
	if currentConn != nil {
		err := currentConn.Close()
		if err != nil {
			logrus.Errorf("Failed to close RabbitMQ connection: %s", err)
		}
	}
}

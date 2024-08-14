package reporter

import (
	"context"
	"encoding/json"
	"github.com/adshao/go-binance/v2"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"price-reporter/protocol"
	"price-reporter/rabbitmq"
	"price-reporter/schedule"
	"sync"
	"time"
)

type Reporter interface {
	Start()
	Close()
}

func NewReporter() Reporter {
	exchangeName := viper.GetString("reporter.exchangeName")
	symbols := viper.GetStringSlice("reporter.symbols")
	return &reporter{
		symbols:      symbols,
		exchangeName: exchangeName,
		wsChanMap:    make(map[string]wsTradeChanPair),
	}
}

type wsTradeChanPair struct {
	stop chan struct{}
	done chan struct{}
}

type reporter struct {
	exchangeName string
	ch           *amqp091.Channel
	symbols      []string
	wsChanMap    map[string]wsTradeChanPair
}

func (r *reporter) renewChannel() error {
	var err error
	r.ch, err = rabbitmq.NewChannel()
	if err != nil {
		return err
	}

	err = r.ch.ExchangeDeclare(
		r.exchangeName, // name
		"topic",        // type
		false,          // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *reporter) Start() {
	err := r.renewChannel()
	if err != nil {
		logrus.Panicf("Failed to open a channel: %s", err)
	}

	for i := range r.symbols {
		err = r.openWsTradeServe(r.symbols[i])
		if err != nil {
			logrus.Panicf("Failed to open ws trade serve: %s", err)
		}
	}

	return
}

func (r *reporter) openWsTradeServe(symbol string) error {
	doneBinanceWsTradeChan, stopBinanceWsTradeChan, wsTradeServeErr := binance.WsTradeServe(symbol, r.newWsTradeHandler(symbol), r.wsTradeErrHandler(symbol))
	if wsTradeServeErr != nil {
		return wsTradeServeErr
	}
	r.wsChanMap[symbol] = wsTradeChanPair{
		stop: stopBinanceWsTradeChan,
		done: doneBinanceWsTradeChan,
	}
	return nil
}

const Timeout = time.Second

func (r *reporter) Close() {
	for symbol, tradeChanPair := range r.wsChanMap {
		close(tradeChanPair.stop)
		select {
		case <-tradeChanPair.done:
			logrus.Infof("Reporter for symbols \"%s\" ended", symbol)
		case <-time.After(Timeout):
			logrus.Errorf("Reporter for symbols \"%s\" didn't end in time (%s)", symbol, Timeout.String())
		}
	}

	errClose := r.ch.Close()
	if errClose != nil {
		logrus.Errorf("Failed to close channel: %s", errClose)
	}
	return
}

func (r *reporter) makeBody(event *binance.WsTradeEvent) ([]byte, error) {
	b := protocol.CoinPriceBody{
		WsTradeEvent: *event,
	}
	return json.Marshal(b)
}

func (r *reporter) newWsTradeHandler(symbol string) func(event *binance.WsTradeEvent) {
	return func(event *binance.WsTradeEvent) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		bodyBinnary, err := r.makeBody(event)
		if err != nil {
			logrus.Errorf("Failed to marshal event: %s", err)
			return
		}
		logrus.Infof(" [%s] Sent %s\n", symbol, string(bodyBinnary))

		r.publishMessage(ctx, symbol, bodyBinnary)
	}
}

const RETRY_INTERVAL = time.Second

func (r *reporter) wsTradeErrHandler(symbol string) func(err error) {
	once := sync.Once{}
	return func(err error) {
		logrus.Errorf("WsTradeServe error[%s]: %s", symbol, err)
		once.Do(func() {
			close(r.wsChanMap[symbol].stop)
			<-r.wsChanMap[symbol].done
			delete(r.wsChanMap, symbol)
			done := schedule.Retry(context.Background(), 3, RETRY_INTERVAL, func(round int) bool {
				err = r.openWsTradeServe(symbol)
				if err != nil {
					logrus.Errorf("Failed to open ws trade serve[%d]: %s", round, err)
					return false
				}
				return true
			})
			if !done {
				logrus.Panicf("Failed to re-open ws trade serve for symbol %s after %d retries", symbol, 3)
			}
		})
	}
}

func (r *reporter) publishMessage(ctx context.Context, symbol string, binary []byte) {
	err := r.ch.PublishWithContext(ctx,
		r.exchangeName, // exchange
		symbol,         // routing key
		false,          // mandatory
		false,          // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        binary,
		})
	if err != nil {
		logrus.Errorf("Failed to publish a message: %s", err)
		done := schedule.Retry(ctx, 3, RETRY_INTERVAL, func(round int) bool {
			logrus.Warnf("Try renew channel[%d]", round)
			renewChannelErr := r.renewChannel()
			if renewChannelErr != nil {
				logrus.Errorf("Failed to renew channe[%d]: %s", round, renewChannelErr)
				return false
			}
			return true
		})
		if !done {
			logrus.Panicf("Failed to renew channel")
		}
		r.publishMessage(ctx, symbol, binary)
	}
}

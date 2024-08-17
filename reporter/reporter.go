package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/adshao/go-binance/v2"
	"github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"price-reporter/protocol"
	"price-reporter/rabbitmq"
	"price-reporter/schedule"
	"sync"
	"sync/atomic"
	"time"
)

const PublishMessageTimeout = time.Second * 5
const PublishMessageRetryInterval = time.Second
const BinanceWSTradeCloseTimeout = time.Second * 5
const RetryOpenWsTradeServeInterval = time.Second

type Reporter interface {
	Start(ctx context.Context) error
	Close()
}

func NewReporter() Reporter {
	exchangeName := viper.GetString("reporter.exchangeName")
	symbols := viper.GetStringSlice("reporter.symbols")
	return &reporter{
		symbols:               symbols,
		exchangeName:          exchangeName,
		binanceWSTradeChanMap: make(map[string]binanceWSTradeChanPair),
	}
}

type binanceWSTradeChanPair struct {
	stop chan struct{}
	done chan struct{}
}

type reporter struct {
	exchangeName              string
	ch                        atomic.Pointer[amqp091.Channel]
	symbols                   []string
	binanceWSTradeChanMap     map[string]binanceWSTradeChanPair
	binanceWSTradeChanMapLock sync.Mutex
}

func (r *reporter) renewChannel(ctx context.Context) error {
	newCh, err := rabbitmq.NewChannel(ctx)
	if err != nil {
		return err
	}

	err = newCh.ExchangeDeclare(
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

	old := r.ch.Swap(newCh)
	if old != nil {
		err = old.Close()
		if err != nil {
			logrus.Errorf("fail to close old channel %s", err)
		}
	}
	return nil
}

func (r *reporter) Start(ctx context.Context) error {
	err := r.renewChannel(ctx)
	if err != nil {
		return err
	}

	for i := range r.symbols {
		err = r.openWsTradeServe(r.symbols[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *reporter) openWsTradeServe(symbol string) error {
	doneBinanceWsTradeChan, stopBinanceWsTradeChan, wsTradeServeErr := binance.WsTradeServe(symbol, r.newWsTradeHandler(symbol), r.wsTradeErrHandler(symbol))
	if wsTradeServeErr != nil {
		return wsTradeServeErr
	}
	r.binanceWSTradeChanMapLock.Lock()
	defer r.binanceWSTradeChanMapLock.Unlock()
	r.binanceWSTradeChanMap[symbol] = binanceWSTradeChanPair{
		stop: stopBinanceWsTradeChan,
		done: doneBinanceWsTradeChan,
	}
	return nil
}

func (r *reporter) Close() {
	r.binanceWSTradeChanMapLock.Lock()
	defer r.binanceWSTradeChanMapLock.Unlock()
	for symbol, tradeChanPair := range r.binanceWSTradeChanMap {
		close(tradeChanPair.stop)
		select {
		case <-tradeChanPair.done:
			logrus.Infof("Reporter for symbols \"%s\" ended", symbol)
		case <-time.After(BinanceWSTradeCloseTimeout):
			logrus.Errorf("Reporter for symbols \"%s\" didn't end in time (%s)", symbol, BinanceWSTradeCloseTimeout.String())
		}
		delete(r.binanceWSTradeChanMap, symbol)
	}
	ch := r.ch.Load()
	if ch != nil {
		errClose := ch.Close()
		if errClose != nil {
			logrus.Errorf("Failed to close channel: %s", errClose)
		}
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
		ctx, cancel := context.WithTimeoutCause(context.Background(), PublishMessageTimeout, fmt.Errorf("publish message timeout"))
		defer cancel()

		bodyBinary, err := r.makeBody(event)
		if err != nil {
			logrus.Errorf("Failed to marshal event: %s", err)
			return
		}
		logrus.Infof(" [%s] Sent %s\n", symbol, string(bodyBinary))

		r.publishMessage(ctx, symbol, bodyBinary)
	}
}

func (r *reporter) stopAndDeleteWsTradeChanPair(symbol string) {
	r.binanceWSTradeChanMapLock.Lock()
	defer r.binanceWSTradeChanMapLock.Unlock()
	_, exists := r.binanceWSTradeChanMap[symbol]
	if !exists {
		return
	}
	close(r.binanceWSTradeChanMap[symbol].stop)
	<-r.binanceWSTradeChanMap[symbol].done
	delete(r.binanceWSTradeChanMap, symbol)
}

func (r *reporter) wsTradeErrHandler(symbol string) func(err error) {
	once := sync.Once{}
	return func(err error) {
		logrus.Errorf("WsTradeServe error[%s]: %s", symbol, err)
		once.Do(func() {
			r.stopAndDeleteWsTradeChanPair(symbol)
			err = schedule.Retry(context.Background(), -1, RetryOpenWsTradeServeInterval, func(round int) bool {
				err = r.openWsTradeServe(symbol)
				if err != nil {
					logrus.Errorf("Failed to open ws trade serve[%d]: %s", round, err)
					return false
				}
				return true
			})
			// should never happen
			if err != nil {
				logrus.Fatalf("program exit because failed to open ws trade, and it sould retry indefinitely")
			}
		})
	}
}

func (r *reporter) publishMessage(ctx context.Context, symbol string, binary []byte) {
	var err error
	ch := r.ch.Load()
	if ch == nil {
		err = fmt.Errorf("channel is nil")
	} else {
		err = ch.PublishWithContext(ctx,
			r.exchangeName, // exchange
			symbol,         // routing key
			false,          // mandatory
			false,          // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        binary,
			})
	}

	if err != nil {
		logrus.Errorf("Failed to publish a message: %s", err)
		err = schedule.Retry(ctx, 3, PublishMessageRetryInterval, func(round int) bool {
			logrus.Warnf("Try renew channel[%d]", round)
			renewChannelErr := r.renewChannel(ctx)
			if renewChannelErr != nil {
				logrus.Errorf("Failed to renew channe[%d]: %s", round, renewChannelErr)
				return false
			}
			return true
		})
		if err != nil {
			logrus.Errorf("Failed to renew channel after retry: %s", err)
		}
	}
}

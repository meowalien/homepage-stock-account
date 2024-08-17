package protocol

import "github.com/adshao/go-binance/v2"

type CoinPriceBody struct {
	WsTradeEvent binance.WsTradeEvent `json:"wsTradeEvent"`
}

package api

import (
	"time"
)

type ExchangePair struct {
	Exchange string
	Pair     string
}

type OrderDataPoint struct {
	OrderType string // buy/bid or sell/ask
	Price     float64
	Volume    float64
	OrderTime time.Time
}

type ExchangeAskBid struct {
	Name string
	Ask  OrderDataPoint
	Bid  OrderDataPoint
}

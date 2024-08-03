package exchanges

import (
	"context"

	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/shopspring/decimal"
)

type Pair struct {
	StandardSymbol  string
	ExchangePairs   []ExchangePair
	ProfitThreshold decimal.Decimal
}

type ExchangePair struct {
	Exchange string
	Symbol   string
}

type ExchangeClient interface {
	Name() string
	Connect() error
	Disconnect()
	RegisterOrderBook(symbol string, ob *orderbook.OrderBook)
	GetOrderBook(symbol string) *orderbook.OrderBook
	Subscribe(pairs []string) error
	ReadMessages(ctx context.Context) error
}

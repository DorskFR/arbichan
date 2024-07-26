package exchanges

import (
	"context"

	"github.com/dorskfr/arbichan/internal/orderbook"
)

type Pair struct {
	StandardSymbol string
	ExchangePairs  []ExchangePair
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

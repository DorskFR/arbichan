package arbitrage

import (
	"context"
	"fmt"
	"sync"

	"github.com/dorskfr/arbichan/internal/exchanges"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/rs/zerolog/log"
)

type ArbitrageDetector struct {
	pairs        map[string][]exchanges.ExchangePair // key is StandardSymbol
	orderBooks   map[string]*orderbook.OrderBook     // key is "exchange:symbol"
	majorUpdates chan orderbook.MajorUpdate
	mu           sync.RWMutex
}

func NewArbitrageDetector(pairs []exchanges.Pair, majorUpdatesChan chan orderbook.MajorUpdate) *ArbitrageDetector {
	ad := &ArbitrageDetector{
		pairs:        make(map[string][]exchanges.ExchangePair),
		orderBooks:   make(map[string]*orderbook.OrderBook),
		majorUpdates: majorUpdatesChan,
	}

	for _, pair := range pairs {
		ad.pairs[pair.StandardSymbol] = pair.ExchangePairs
	}

	return ad
}

func (ad *ArbitrageDetector) RegisterOrderBook(exchange, symbol string, ob *orderbook.OrderBook) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	key := fmt.Sprintf("%s:%s", exchange, symbol)
	ad.orderBooks[key] = ob
}

func (ad *ArbitrageDetector) Run(ctx context.Context) {
	for {
		select {
		case update := <-ad.majorUpdates:
			ad.checkArbitrage(update.Exchange, update.Symbol)
		case <-ctx.Done():
			return
		}
	}
}

func (ad *ArbitrageDetector) checkArbitrage(exchange, symbol string) {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	var pairSymbol string
	for ps, eps := range ad.pairs {
		for _, ep := range eps {
			if ep.Exchange == exchange && ep.Symbol == symbol {
				pairSymbol = ps
				break
			}
		}
		if pairSymbol != "" {
			break
		}
	}

	if pairSymbol == "" {
		log.Warn().Str("exchange", exchange).Str("symbol", symbol).Msg("Received update for unknown pair")
		return
	}

	ad.checkArbitrageForPair(pairSymbol)
}

func (ad *ArbitrageDetector) checkArbitrageForPair(pairSymbol string) {
	exchangePairs := ad.pairs[pairSymbol]
	for i := 0; i < len(exchangePairs)-1; i++ {
		for j := i + 1; j < len(exchangePairs); j++ {
			ad.compareExchanges(pairSymbol, exchangePairs[i], exchangePairs[j])
		}
	}
}

func (ad *ArbitrageDetector) compareExchanges(pairSymbol string, ep1, ep2 exchanges.ExchangePair) {
	key1 := fmt.Sprintf("%s:%s", ep1.Exchange, ep1.Symbol)
	key2 := fmt.Sprintf("%s:%s", ep2.Exchange, ep2.Symbol)

	ob1 := ad.orderBooks[key1]
	ob2 := ad.orderBooks[key2]

	if ob1 == nil || ob2 == nil {
		log.Warn().Str("pair", pairSymbol).Msg("Missing order book for comparison")
		return
	}

	bestBid1, bestAsk1 := ob1.BestBidAsk()
	bestBid2, bestAsk2 := ob2.BestBidAsk()

	if bestBid1 > bestAsk2 {
		profit := bestBid1 - bestAsk2
		log.Info().
			Str("pair", pairSymbol).
			Str("buy", ep2.Exchange).
			Str("sell", ep1.Exchange).
			Float64("profit", profit).
			Msg("Arbitrage opportunity detected")
	}

	if bestBid2 > bestAsk1 {
		profit := bestBid2 - bestAsk1
		log.Info().
			Str("pair", pairSymbol).
			Str("buy", ep1.Exchange).
			Str("sell", ep2.Exchange).
			Float64("profit", profit).
			Msg("Arbitrage opportunity detected")
	}
}

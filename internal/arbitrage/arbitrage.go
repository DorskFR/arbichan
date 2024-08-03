package arbitrage

import (
	"context"
	"fmt"
	"sync"

	"github.com/dorskfr/arbichan/internal/exchanges"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

type ArbitrageDetector struct {
	pairs           map[string][]exchanges.ExchangePair // key is StandardSymbol
	orderBooks      map[string]*orderbook.OrderBook     // key is "exchange:symbol"
	profitThreshold map[string]decimal.Decimal          // key is StandardSymbol
	majorUpdates    chan orderbook.MajorUpdate
	mu              sync.RWMutex
}

func NewArbitrageDetector(pairs []exchanges.Pair, majorUpdatesChan chan orderbook.MajorUpdate) *ArbitrageDetector {
	ad := &ArbitrageDetector{
		pairs:           make(map[string][]exchanges.ExchangePair),
		orderBooks:      make(map[string]*orderbook.OrderBook),
		profitThreshold: make(map[string]decimal.Decimal),
		majorUpdates:    majorUpdatesChan,
	}

	for _, pair := range pairs {
		ad.pairs[pair.StandardSymbol] = pair.ExchangePairs
		ad.profitThreshold[pair.StandardSymbol] = decimal.Zero
	}

	return ad
}

func (ad *ArbitrageDetector) RegisterOrderBook(exchange, symbol string, ob *orderbook.OrderBook) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	key := fmt.Sprintf("%s:%s", exchange, symbol)
	ad.orderBooks[key] = ob
}

func (ad *ArbitrageDetector) SetProfitThreshold(symbol string, threshold decimal.Decimal) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	ad.profitThreshold[symbol] = threshold
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
	if bestBid1.IsZero() || bestAsk1.IsZero() || bestBid2.IsZero() || bestAsk2.IsZero() {
		return
	}

	profitThreshold := ad.profitThreshold[pairSymbol]

	if bestBid1.GreaterThan(bestAsk2) {
		profit := bestBid1.Sub(bestAsk2)
		profitPercentage := profit.Div(bestAsk2).Mul(decimal.NewFromInt(100))
		if profitPercentage.GreaterThanOrEqual(profitThreshold) {
			log.Info().Msgf(
				"Arbitrage opportunity detected for %s: Buy on %s at %s, Sell on %s at %s. Profit: %s (%s%%) > (%s%%)",
				pairSymbol,
				ep2.Exchange,
				bestAsk2.String(),
				ep1.Exchange,
				bestBid1.String(),
				profit.String(),
				profitPercentage.StringFixed(2),
				profitThreshold.StringFixed(2),
			)
			return
		}
	}

	if bestBid2.GreaterThan(bestAsk1) {
		profit := bestBid2.Sub(bestAsk1)
		profitPercentage := profit.Div(bestAsk1).Mul(decimal.NewFromInt(100))
		if profitPercentage.GreaterThanOrEqual(profitThreshold) {
			log.Info().Msgf(
				"Arbitrage opportunity detected for %s: Buy on %s at %s, Sell on %s at %s. Profit: %s (%s%%) > (%s%%)",
				pairSymbol,
				ep1.Exchange,
				bestAsk1.String(),
				ep2.Exchange,
				bestBid2.String(),
				profit.String(),
				profitPercentage.StringFixed(2),
				profitThreshold.StringFixed(2),
			)
			return
		}
	}

}

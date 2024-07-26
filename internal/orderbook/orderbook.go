package orderbook

import (
	"context"
	"sync"
)

type OrderBookUpdate struct {
	Type   string // "bid" or "ask"
	Price  float64
	Amount float64
}

type MajorUpdate struct {
	Exchange string
	Symbol   string
}

type OrderBook struct {
	exchange     string
	symbol       string
	bids         map[float64]float64
	asks         map[float64]float64
	bestBid      float64
	bestAsk      float64
	mu           sync.RWMutex
	Updates      chan OrderBookUpdate
	MajorUpdates chan MajorUpdate
}

func NewOrderBook(exchange, symbol string, updateChan chan OrderBookUpdate, majorUpdateChan chan MajorUpdate) *OrderBook {
	return &OrderBook{
		exchange:     exchange,
		symbol:       symbol,
		bids:         make(map[float64]float64),
		asks:         make(map[float64]float64),
		Updates:      updateChan,
		MajorUpdates: majorUpdateChan,
	}
}

func (ob *OrderBook) Run(ctx context.Context) {
	for {
		select {
		case update := <-ob.Updates:
			if ob.applyUpdate(update) {
				ob.MajorUpdates <- MajorUpdate{ob.exchange, ob.symbol}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (ob *OrderBook) BestBidAsk() (float64, float64) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()
	return ob.bestBid, ob.bestAsk
}

func (ob *OrderBook) applyUpdate(update OrderBookUpdate) bool {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	var isMajor bool
	switch update.Type {
	case "bid":
		isMajor = ob.updateBids(update.Price, update.Amount)
	case "ask":
		isMajor = ob.updateAsks(update.Price, update.Amount)
	}

	return isMajor
}

func (ob *OrderBook) updateBids(price, amount float64) bool {
	if amount == 0 {
		delete(ob.bids, price)
	} else {
		ob.bids[price] = amount
	}

	newBestBid := getMaxKey(ob.bids)
	isMajor := newBestBid != ob.bestBid
	ob.bestBid = newBestBid
	return isMajor
}

func (ob *OrderBook) updateAsks(price, amount float64) bool {
	if amount == 0 {
		delete(ob.asks, price)
	} else {
		ob.asks[price] = amount
	}

	newBestAsk := getMinKey(ob.asks)
	isMajor := newBestAsk != ob.bestAsk
	ob.bestAsk = newBestAsk
	return isMajor
}

func getMaxKey(m map[float64]float64) float64 {
	var max float64
	for k := range m {
		if k > max {
			max = k
		}
	}
	return max
}

func getMinKey(m map[float64]float64) float64 {
	var min float64
	first := true
	for k := range m {
		if first || k < min {
			min = k
			first = false
		}
	}
	return min
}

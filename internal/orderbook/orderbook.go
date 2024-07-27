package orderbook

import (
	"context"
	"sort"
	"sync"

	"github.com/shopspring/decimal"
)

// MajorUpdate represents a significant change in the order book
type MajorUpdate struct {
	Exchange string // Name of the exchange
	Symbol   string // Trading pair symbol
}

// PriceLevel represents a single price level in the order book
type PriceLevel struct {
	Type      string // "bid" or "ask"
	Price     decimal.Decimal
	Amount    decimal.Decimal
	PriceStr  string
	AmountStr string
}

// OrderBook represents the full order book for a trading pair
type OrderBook struct {
	exchange           string
	symbol             string
	depth              int              // Maximum number of price levels to maintain
	bids               []PriceLevel     // Sorted list of bid price levels
	asks               []PriceLevel     // Sorted list of ask price levels
	mu                 sync.RWMutex     // Mutex for thread-safe operations
	Updates            chan PriceLevel  // Channel for receiving updates
	MajorUpdates       chan MajorUpdate // Channel for sending notifications of major changes
	ProcessingComplete chan struct{}    // Channel for signaling when all updates are processed
}

// NewOrderBook creates and initializes a new OrderBook
func NewOrderBook(exchange, symbol string, depth int, updateChan chan PriceLevel, majorUpdateChan chan MajorUpdate) *OrderBook {
	return &OrderBook{
		exchange:           exchange,
		symbol:             symbol,
		depth:              depth,
		bids:               make([]PriceLevel, 0),
		asks:               make([]PriceLevel, 0),
		Updates:            updateChan,
		MajorUpdates:       majorUpdateChan,
		ProcessingComplete: make(chan struct{}),
	}
}

// Run starts the main loop of the OrderBook
func (ob *OrderBook) Run(ctx context.Context) {
	for {
		select {
		case update := <-ob.Updates:
			// Process all updates
			ob.applyUpdate(update)
		case <-ob.ProcessingComplete:
		out:
			for {
				select {
				case update := <-ob.Updates:
					ob.applyUpdate(update)
				default:
					break out
				}
			}
			if ob.reprocessLevels(&ob.bids, true) || ob.reprocessLevels(&ob.asks, false) {
				// If it's a major change, send a notification
				ob.MajorUpdates <- MajorUpdate{ob.exchange, ob.symbol}
			}
			// Signal that all pending updates have been processed
			ob.ProcessingComplete <- struct{}{}
		case <-ctx.Done():
			// Exit the loop if the context is cancelled
			return
		}
	}
}

// GetTopLevels returns copies of the top bid and ask levels
func (ob *OrderBook) GetTopLevels() ([]PriceLevel, []PriceLevel) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	// Create new slices to avoid exposing internal state
	bids := make([]PriceLevel, len(ob.bids))
	asks := make([]PriceLevel, len(ob.asks))

	// Copy the current state of bids and asks
	copy(bids, ob.bids)
	copy(asks, ob.asks)

	return bids, asks
}

// BestBidAsk returns the best bid and ask prices
func (ob *OrderBook) BestBidAsk() (decimal.Decimal, decimal.Decimal) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	if len(ob.bids) > 0 && len(ob.asks) > 0 {
		return ob.bids[0].Price, ob.asks[0].Price
	}
	return decimal.Zero, decimal.Zero // Return 0, 0 if there are no bids or asks
}

// applyUpdate applies a single update to the order book
func (ob *OrderBook) applyUpdate(update PriceLevel) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	if update.Type == "bid" {
		ob.updateLevels(update, &ob.bids)
	} else if update.Type == "ask" {
		ob.updateLevels(update, &ob.asks)
	}
}

// updateLevels updates either the bid or ask levels
func (ob *OrderBook) updateLevels(update PriceLevel, levels *[]PriceLevel) {

	for i, level := range *levels {
		if level.Price.Equal(update.Price) {
			if update.Amount.IsZero() {
				// Remove the level
				*levels = append((*levels)[:i], (*levels)[i+1:]...)
			} else {
				// Update the existing level
				(*levels)[i].Amount = update.Amount
				(*levels)[i].AmountStr = update.AmountStr
			}
			return
		}
	}

	// If we're here, the price level doesn't exist
	if !update.Amount.IsZero() {
		// Append the new price level
		*levels = append(*levels, update)
	}
}

func (ob *OrderBook) reprocessLevels(levels *[]PriceLevel, isDescending bool) bool {
	oldBest := decimal.Zero
	if len(*levels) > 0 {
		oldBest = (*levels)[0].Price
	}

	sort.Slice(*levels, func(i, j int) bool {
		if isDescending {
			return (*levels)[i].Price.Compare((*levels)[j].Price) == 1 // Sort bids in descending order
		}
		return (*levels)[i].Price.Compare((*levels)[j].Price) == -1 // Sort asks in ascending order
	})

	// Truncate
	if len(*levels) > ob.depth {
		*levels = (*levels)[:ob.depth]
	}

	newBest := decimal.Zero
	if len(*levels) > 0 {
		newBest = (*levels)[0].Price
	}

	return !oldBest.Equal(newBest)
}

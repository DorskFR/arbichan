package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const (
	binanceAPIWSURL    = "wss://ws-api.binance.com:443/ws-api/v3"
	binanceStreamWSURL = "wss://stream.binance.com:9443/ws"
)

type BinanceClient struct {
	name          string
	streamConn    *websocket.Conn
	apiConn       *websocket.Conn
	orderBooks    map[string]*orderbook.OrderBook
	mu            sync.RWMutex
	lastUpdateIDs map[string]int64
}

type binanceWSRequest struct {
	ID     int         `json:"id"`
	Method string      `json:"method"`
	Params interface{} `json:"params"`
}

type binanceWSResponse struct {
	ID     int             `json:"id"`
	Status int             `json:"status"`
	Result json.RawMessage `json:"result"`
}

type BinanceSubscriptionRequest struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     int      `json:"id"`
}

type binanceDepthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

type binanceDepthUpdate struct {
	EventType     string     `json:"e"`
	EventTime     int64      `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Bids          [][]string `json:"b"`
	Asks          [][]string `json:"a"`
}

func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		name:          "binance",
		orderBooks:    make(map[string]*orderbook.OrderBook),
		lastUpdateIDs: make(map[string]int64),
	}
}

func (c *BinanceClient) Name() string {
	return c.name
}

func (c *BinanceClient) Connect() error {
	var err error
	c.streamConn, _, err = websocket.DefaultDialer.Dial(binanceStreamWSURL, nil)
	if err != nil {
		return fmt.Errorf("error connecting to Binance Stream WebSocket: %w", err)
	}

	c.apiConn, _, err = websocket.DefaultDialer.Dial(binanceAPIWSURL, nil)
	if err != nil {
		return fmt.Errorf("error connecting to Binance API WebSocket: %w", err)
	}

	return nil
}

func (c *BinanceClient) Disconnect() {
	log.Info().Str("exchange", c.name).Msg("Disconnecting")
	if c.streamConn != nil {
		c.streamConn.Close()
	}
	if c.apiConn != nil {
		c.apiConn.Close()
	}
}

func (c *BinanceClient) RegisterOrderBook(symbol string, ob *orderbook.OrderBook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orderBooks[symbol] = ob
}

func (c *BinanceClient) GetOrderBook(symbol string) *orderbook.OrderBook {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.orderBooks[symbol]
}

func (c *BinanceClient) Subscribe(symbols []string) error {
	for _, symbol := range symbols {
		// First, get the depth snapshot
		if err := c.getDepthSnapshot(symbol); err != nil {
			return fmt.Errorf("error getting depth snapshot for %s: %w", symbol, err)
		}
	}

	// Then, subscribe to the depth stream
	streamParams := make([]string, len(symbols))
	for i, symbol := range symbols {
		streamParams[i] = fmt.Sprintf("%s@depth@100ms", symbol)
	}

	subscription := BinanceSubscriptionRequest{
		Method: "SUBSCRIBE",
		Params: streamParams,
		ID:     1,
	}

	if err := c.streamConn.WriteJSON(subscription); err != nil {
		return fmt.Errorf("error subscribing to depth streams: %w", err)
	}

	return nil
}

func (c *BinanceClient) ReadMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, message, err := c.streamConn.ReadMessage()
			if err != nil {
				return fmt.Errorf("error reading message: %w", err)
			}

			var update binanceDepthUpdate
			if err := json.Unmarshal(message, &update); err != nil {
				log.Error().Err(err).Str("exchange", c.name).Msg("Error unmarshalling message")
				continue
			}

			if update.Symbol == "" {
				log.Error().Str("exchange", c.name).Str("message", string(message)).Msg("The message was not parsed correctly")
				continue
			}

			c.handleDepthUpdate(update)
		}
	}
}

func (c *BinanceClient) getDepthSnapshot(symbol string) error {
	req := binanceWSRequest{
		ID:     time.Now().Nanosecond(),
		Method: "depth",
		Params: map[string]interface{}{
			"symbol": symbol,
			"limit":  5,
		},
	}

	log.Debug().Str("exchange", c.name).Str("symbol", symbol).Msg("Requesting depth snapshot")
	if err := c.apiConn.WriteJSON(req); err != nil {
		return fmt.Errorf("error requesting depth snapshot: %w", err)
	}

	_, msg, err := c.apiConn.ReadMessage()
	if err != nil {
		return fmt.Errorf("error reading depth snapshot response: %w", err)
	}

	var resp binanceWSResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return fmt.Errorf("error unmarshalling depth snapshot response: %w", err)
	}

	var snapshot binanceDepthSnapshot
	if err := json.Unmarshal(resp.Result, &snapshot); err != nil {
		return fmt.Errorf("error unmarshalling depth snapshot data: %w", err)
	}

	c.applyDepthSnapshot(symbol, &snapshot)
	return nil
}

func (c *BinanceClient) applyDepthSnapshot(symbol string, snapshot *binanceDepthSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ob, exists := c.orderBooks[symbol]
	if !exists {
		log.Warn().Str("exchange", c.name).Str("symbol", symbol).Msg("Order book not found for symbol")
		return
	}

	// TODO: Clear existing order book data. Not handled yet.
	ob.Updates <- orderbook.PriceLevel{Type: "clear"}

	// Apply snapshot data
	for _, bid := range snapshot.Bids {
		price, _ := decimal.NewFromString(bid[0])
		amount, _ := decimal.NewFromString(bid[1])
		ob.Updates <- orderbook.PriceLevel{Type: "bid", Price: price, Amount: amount, PriceStr: price.String(), AmountStr: amount.String()}
	}
	for _, ask := range snapshot.Asks {
		price, _ := decimal.NewFromString(ask[0])
		amount, _ := decimal.NewFromString(ask[1])
		ob.Updates <- orderbook.PriceLevel{Type: "ask", Price: price, Amount: amount, PriceStr: price.String(), AmountStr: amount.String()}
	}

	c.lastUpdateIDs[symbol] = snapshot.LastUpdateID
}

func (c *BinanceClient) handleDepthUpdate(update binanceDepthUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ob, exists := c.orderBooks[update.Symbol]
	if !exists {
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Order book not found for symbol")
		return
	}

	lastUpdateID, exists := c.lastUpdateIDs[update.Symbol]
	if !exists {
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Last update ID not found for symbol")
		return
	}

	if update.FirstUpdateID <= lastUpdateID+1 && update.FinalUpdateID >= lastUpdateID+1 {
		c.applyDepthUpdate(ob, update)
		c.lastUpdateIDs[update.Symbol] = update.FinalUpdateID
	} else if update.FinalUpdateID < lastUpdateID+1 {
		// Discard this update
		return
	} else {
		// We've missed some updates, need to resync
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Missed updates, resyncing")
		if err := c.getDepthSnapshot(update.Symbol); err != nil {
			log.Error().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Error resyncing")
		}
	}
}

func (c *BinanceClient) applyDepthUpdate(ob *orderbook.OrderBook, update binanceDepthUpdate) {
	for _, bid := range update.Bids {
		price, _ := decimal.NewFromString(bid[0])
		amount, _ := decimal.NewFromString(bid[1])
		ob.Updates <- orderbook.PriceLevel{Type: "bid", Price: price, Amount: amount, PriceStr: price.String(), AmountStr: amount.String()}
	}
	for _, ask := range update.Asks {
		price, _ := decimal.NewFromString(ask[0])
		amount, _ := decimal.NewFromString(ask[1])
		ob.Updates <- orderbook.PriceLevel{Type: "ask", Price: price, Amount: amount, PriceStr: price.String(), AmountStr: amount.String()}
	}
}

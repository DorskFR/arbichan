package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
)

const (
	binanceStreamWSURL = "wss://stream.binance.com:9443/ws"
	binanceAPIWSURL    = "wss://ws-api.binance.com:443/ws-api/v3"
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

	subscribeMsg := struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int      `json:"id"`
	}{
		Method: "SUBSCRIBE",
		Params: streamParams,
		ID:     1,
	}

	if err := c.streamConn.WriteJSON(subscribeMsg); err != nil {
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
				log.Printf("Error unmarshalling message: %v", err)
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
			"limit":  1000,
		},
	}

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
		log.Printf("Order book for symbol %s not found", symbol)
		return
	}

	// Clear existing order book data
	ob.Updates <- orderbook.OrderBookUpdate{Type: "clear"}

	// Apply snapshot data
	for _, bid := range snapshot.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		amount, _ := strconv.ParseFloat(bid[1], 64)
		ob.Updates <- orderbook.OrderBookUpdate{Type: "bid", Price: price, Amount: amount}
	}
	for _, ask := range snapshot.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		amount, _ := strconv.ParseFloat(ask[1], 64)
		ob.Updates <- orderbook.OrderBookUpdate{Type: "ask", Price: price, Amount: amount}
	}

	c.lastUpdateIDs[symbol] = snapshot.LastUpdateID
}

func (c *BinanceClient) handleDepthUpdate(update binanceDepthUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ob, exists := c.orderBooks[update.Symbol]
	if !exists {
		return
	}

	lastUpdateID, exists := c.lastUpdateIDs[update.Symbol]
	if !exists {
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
		log.Printf("Missed updates for %s, resyncing", update.Symbol)
		if err := c.getDepthSnapshot(update.Symbol); err != nil {
			log.Printf("Error resyncing %s: %v", update.Symbol, err)
		}
	}
}

func (c *BinanceClient) applyDepthUpdate(ob *orderbook.OrderBook, update binanceDepthUpdate) {
	for _, bid := range update.Bids {
		price, _ := strconv.ParseFloat(bid[0], 64)
		amount, _ := strconv.ParseFloat(bid[1], 64)
		ob.Updates <- orderbook.OrderBookUpdate{Type: "bid", Price: price, Amount: amount}
	}
	for _, ask := range update.Asks {
		price, _ := strconv.ParseFloat(ask[0], 64)
		amount, _ := strconv.ParseFloat(ask[1], 64)
		ob.Updates <- orderbook.OrderBookUpdate{Type: "ask", Price: price, Amount: amount}
	}
}

package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
)

const (
	krakenWSURL = "wss://ws.kraken.com"
)

type KrakenClient struct {
	name       string
	conn       *websocket.Conn
	orderBooks map[string]*orderbook.OrderBook
	mu         sync.RWMutex
}

func NewKrakenClient() *KrakenClient {
	return &KrakenClient{
		name:       "kraken",
		orderBooks: make(map[string]*orderbook.OrderBook),
	}
}

func (c *KrakenClient) Name() string {
	return c.name
}

func (c *KrakenClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(krakenWSURL, nil)
	if err != nil {
		return fmt.Errorf("error connecting to Kraken WebSocket: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *KrakenClient) Disconnect() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *KrakenClient) RegisterOrderBook(symbol string, ob *orderbook.OrderBook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orderBooks[symbol] = ob
}

func (c *KrakenClient) GetOrderBook(symbol string) *orderbook.OrderBook {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.orderBooks[symbol]
}

func (c *KrakenClient) Subscribe(pairs []string) error {
	subscription := struct {
		Event        string   `json:"event"`
		Pair         []string `json:"pair"`
		Subscription struct {
			Name string `json:"name"`
		} `json:"subscription"`
	}{
		Event: "subscribe",
		Pair:  pairs,
		Subscription: struct {
			Name string `json:"name"`
		}{
			Name: "book",
		},
	}

	return c.conn.WriteJSON(subscription)
}

func (c *KrakenClient) ReadMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("error reading message: %w", err)
			}

			var data interface{}
			if err := json.Unmarshal(message, &data); err != nil {
				log.Printf("Error unmarshalling message: %v", err)
				continue
			}

			switch msg := data.(type) {
			case []interface{}:
				c.handleBookUpdate(msg)
			case map[string]interface{}:
				if event, ok := msg["event"].(string); ok && event == "heartbeat" {
					// Handle heartbeat
					continue
				}
				// Handle other types of messages if needed
			}
		}
	}
}

func (c *KrakenClient) handleBookUpdate(msg []interface{}) {
	if len(msg) < 4 {
		return
	}

	channelName, ok := msg[2].(string)
	if !ok || channelName != "book" {
		return
	}

	pair, ok := msg[3].(string)
	if !ok {
		return
	}

	book, ok := msg[1].(map[string]interface{})
	if !ok {
		return
	}

	c.mu.RLock()
	ob, exists := c.orderBooks[pair]
	c.mu.RUnlock()

	if !exists {
		return
	}

	if asks, ok := book["as"].([]interface{}); ok {
		c.updateLevels(ob, asks, true)
	}
	if bids, ok := book["bs"].([]interface{}); ok {
		c.updateLevels(ob, bids, false)
	}

	if asks, ok := book["a"].([]interface{}); ok {
		c.updateLevels(ob, asks, true)
	}
	if bids, ok := book["b"].([]interface{}); ok {
		c.updateLevels(ob, bids, false)
	}
}

func (c *KrakenClient) updateLevels(ob *orderbook.OrderBook, levels []interface{}, isAsk bool) {
	for _, level := range levels {
		priceLevel, ok := level.([]interface{})
		if !ok || len(priceLevel) < 2 {
			continue
		}

		price, err := strconv.ParseFloat(priceLevel[0].(string), 64)
		if err != nil {
			continue
		}

		amount, err := strconv.ParseFloat(priceLevel[1].(string), 64)
		if err != nil {
			continue
		}

		updateType := "bid"
		if isAsk {
			updateType = "ask"
		}

		ob.Updates <- orderbook.OrderBookUpdate{
			Type:   updateType,
			Price:  price,
			Amount: amount,
		}
	}
}

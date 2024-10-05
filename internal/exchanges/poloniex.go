package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dorskfr/arbichan/internal/messagetracker"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

type PoloniexClient struct {
	*BaseExchangeClient
	messagetracker *messagetracker.MessageTracker
	lastIDs        map[string]int64
}

type poloniexWSRequest struct {
	Event   string   `json:"event"`
	Channel []string `json:"channel"`
	Symbols []string `json:"symbols"`
}

type poloniexWSResponse struct {
	Channel string                    `json:"channel"`
	Event   string                    `json:"event"`
	Symbols []string                  `json:"symbols,omitempty"`
	Data    []poloniexOrderBookUpdate `json:"data,omitempty"`
}

type poloniexOrderBookUpdate struct {
	Symbol     string     `json:"symbol"`
	Asks       [][]string `json:"asks"`
	Bids       [][]string `json:"bids"`
	CreateTime int64      `json:"createTime"`
	LastID     int64      `json:"lastId"`
	ID         int64      `json:"id"`
	Ts         int64      `json:"ts"`
}

func NewPoloniexClient() *PoloniexClient {
	poloniexClient := &PoloniexClient{
		lastIDs:        make(map[string]int64),
		messagetracker: messagetracker.NewMessageTracker("poloniex", time.Minute),
	}
	poloniexClient.BaseExchangeClient = NewBaseExchangeClient("poloniex", "wss://ws.poloniex.com/ws/public", poloniexClient)
	return poloniexClient

}

func (c *PoloniexClient) Subscribe(symbols []string) error {
	channels := make([]string, 1)
	channels[0] = "book_lv2"
	subscribeReq := poloniexWSRequest{
		Event:   "subscribe",
		Channel: channels,
		Symbols: symbols,
	}

	if err := c.conn.WriteJSON(subscribeReq); err != nil {
		return fmt.Errorf("error subscribing to order book: %w", err)
	}

	// Wait for subscription confirmation
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("error reading subscription response: %w", err)
	}

	var response poloniexWSResponse
	if err := json.Unmarshal(msg, &response); err != nil {
		return fmt.Errorf("error unmarshalling subscription response: %w", err)
	}

	if response.Event == "subscribe" && response.Channel == "book_lv2" {
		log.Info().Str("exchange", c.name).Msg("Successfully subscribed to order book")
	} else {
		return fmt.Errorf("unexpected subscription response: %s", string(msg))
	}

	return nil
}

func (c *PoloniexClient) ReadMessages(ctx context.Context) error {
	return c.BaseExchangeClient.ReadMessages(ctx, c.handleMessage, 25*time.Second)
}

func (c *PoloniexClient) handleMessage(message WebSocketMessage) error {
	var response poloniexWSResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	for _, update := range response.Data {
		c.handleOrderBookUpdate(update)
	}
	return nil
}

func (c *PoloniexClient) SendPing() error {
	pingReq := poloniexWSRequest{
		Event: "ping",
	}
	if err := c.conn.WriteJSON(pingReq); err != nil {
		log.Warn().Err(err).Str("exchange", c.name).Msg("Failed to send ping")
		return fmt.Errorf("error sending ping: %w", err)
	}
	return nil
}

func (c *PoloniexClient) handleOrderBookUpdate(update poloniexOrderBookUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ob, exists := c.orderBooks[update.Symbol]
	if !exists {
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Order book not found for symbol")
		return
	}

	lastID, exists := c.lastIDs[update.Symbol]
	if !exists {
		// This is the first update, treat it as a snapshot
		c.applyOrderBookSnapshot(ob, update)
	} else if update.LastID != lastID {
		// We've missed some updates, need to resync
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Missed updates, resyncing")
		// In a real implementation, you might want to re-subscribe or fetch a new snapshot
		return
	} else {
		// Apply the update
		c.applyOrderBookUpdate(ob, update)
	}

	c.lastIDs[update.Symbol] = update.ID

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete
}

func (c *PoloniexClient) applyOrderBookSnapshot(ob *orderbook.OrderBook, snapshot poloniexOrderBookUpdate) {
	// Clear existing order book data
	// TODO: Not implemented yet
	ob.Updates <- orderbook.PriceLevel{Type: "clear"}

	// Apply snapshot data
	for _, bid := range snapshot.Bids {
		price, _ := decimal.NewFromString(bid[0])
		amount, _ := decimal.NewFromString(bid[1])
		ob.Updates <- orderbook.PriceLevel{
			Type:      "bid",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}
	for _, ask := range snapshot.Asks {
		price, _ := decimal.NewFromString(ask[0])
		amount, _ := decimal.NewFromString(ask[1])
		ob.Updates <- orderbook.PriceLevel{
			Type:      "ask",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}
}

func (c *PoloniexClient) applyOrderBookUpdate(ob *orderbook.OrderBook, update poloniexOrderBookUpdate) {
	for _, bid := range update.Bids {
		price, _ := decimal.NewFromString(bid[0])
		amount, _ := decimal.NewFromString(bid[1])
		ob.Updates <- orderbook.PriceLevel{
			Type:      "bid",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}
	for _, ask := range update.Asks {
		price, _ := decimal.NewFromString(ask[0])
		amount, _ := decimal.NewFromString(ask[1])
		ob.Updates <- orderbook.PriceLevel{Type: "ask",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}
}

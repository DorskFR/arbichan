package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/dorskfr/arbichan/internal/messagetracker"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

// Should fit the different messages we can receive
type krakenMessage struct {
	Channel string                `json:"channel,omitempty"`
	Data    json.RawMessage       `json:"data,omitempty"`
	Error   string                `json:"error,omitempty"`
	Method  string                `json:"method,omitempty"`
	Result  krakenSubscriptionAck `json:"result,omitempty"`
	Success *bool                 `json:"success,omitempty"`
	Type    string                `json:"type,omitempty"`
}

// Order book data updates are in this format
type krakenBookUpdate struct {
	Asks      []priceQty `json:"asks,omitempty"`
	Bids      []priceQty `json:"bids,omitempty"`
	Checksum  int64      `json:"checksum"`
	Symbol    string     `json:"symbol"`
	Timestamp string     `json:"timestamp,omitempty"`
}

// Represents a single price and we keep the String representation as this keeps the precision to calculate the checksum
type priceQty struct {
	Price    decimal.Decimal `json:"price"`
	Qty      decimal.Decimal `json:"qty"`
	PriceStr string
	QtyStr   string
}

// We use a custom unmarshaller because we need to keep the precision using strings
func (pq *priceQty) UnmarshalJSON(data []byte) error {
	var temp struct {
		Price json.Number `json:"price"`
		Qty   json.Number `json:"qty"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	var err error
	pq.Price, err = decimal.NewFromString(string(temp.Price))
	if err != nil {
		return err
	}
	pq.PriceStr = string(temp.Price)

	pq.Qty, err = decimal.NewFromString(string(temp.Qty))
	if err != nil {
		return err
	}
	pq.QtyStr = string(temp.Qty)

	return nil
}

type krakenSubscriptionParams struct {
	Channel string   `json:"channel"`
	Symbol  []string `json:"symbol"`
}

type krakenSubscriptionRequest struct {
	Method string                   `json:"method"`
	Params krakenSubscriptionParams `json:"params"`
}

type krakenSubscriptionAck struct {
	Channel  string `json:"book"`
	Depth    int    `json:"depth"`
	Snapshot bool   `json:"snapshot"`
	Symbol   string `json:"symbol"`
}
type KrakenClient struct {
	*BaseExchangeClient
	messagetracker *messagetracker.MessageTracker
}

func NewKrakenClient() *KrakenClient {
	return &KrakenClient{
		BaseExchangeClient: NewBaseExchangeClient("kraken", "wss://ws.kraken.com/v2"),
		messagetracker:     messagetracker.NewMessageTracker("kraken", time.Minute),
	}
}

// https://docs.kraken.com/api/docs/websocket-v2/book/
// https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/
func (c *KrakenClient) Subscribe(pairs []string) error {
	subscription := krakenSubscriptionRequest{
		Method: "subscribe",
		Params: krakenSubscriptionParams{
			Channel: "book",
			Symbol:  pairs,
		},
	}
	log.Info().Str("exchange", c.name).Interface("subscription", subscription).Msg("Sending subscription request")
	return c.conn.WriteJSON(subscription)
}

func (c *KrakenClient) ReadMessages(ctx context.Context) error {
	return c.BaseExchangeClient.ReadMessages(ctx, c.handleMessage)
}

func (c *KrakenClient) handleMessage(message []byte) error {
	var msg krakenMessage
	if err := json.Unmarshal(message, &msg); err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	switch {
	case msg.Error != "":
		log.Error().Str("error", msg.Error).Msg("Could not subscribe")
	case msg.Channel == "book":
		var updates []krakenBookUpdate
		if err := json.Unmarshal(msg.Data, &updates); err != nil {
			return fmt.Errorf("error unmarshalling book data: %w", err)
		}
		for _, update := range updates {
			c.applyBookUpdate(update)
		}
	case msg.Method == "subscribe" && *msg.Success:
		log.Info().Str("exchange", c.Name()).Str("channel", msg.Result.Channel).Str("symbol", msg.Result.Symbol).Msg("Subscription successful")
	case msg.Channel == "status", msg.Channel == "pong", msg.Channel == "heartbeat":
		// Silently ignore these messages
	default:
		log.Warn().Str("exchange", c.Name()).Str("rawMessage", string(message)).Msg("Unhandled message type")
	}
	return nil
}

func (c *KrakenClient) applyBookUpdate(update krakenBookUpdate) {

	// Check that we can apply the message
	c.mu.RLock()
	ob, exists := c.orderBooks[update.Symbol]
	c.mu.RUnlock()
	if !exists {
		log.Warn().Str("exchange", c.name).Str("pair", update.Symbol).Msg("Order book not found for pair")
		return
	}

	// Send the Asks and Bids updates
	c.sendOrderBookUpdates(ob, update.Asks, "ask")
	c.sendOrderBookUpdates(ob, update.Bids, "bid")

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete

	if !c.verifyChecksum(ob, update.Checksum) {
		log.Error().Str("exchange", c.name).Str("pair", update.Symbol).Msg("Checksum verification failed")
		// TODO: Wipe the orderbook and request a new snapshot
	}
}

func (c *KrakenClient) sendOrderBookUpdates(ob *orderbook.OrderBook, updates []priceQty, updateType string) {
	for _, update := range updates {
		ob.Updates <- orderbook.PriceLevel{
			Type:      updateType,
			Price:     update.Price,
			Amount:    update.Qty,
			PriceStr:  update.PriceStr,
			AmountStr: update.QtyStr,
		}
	}
}

// If this fails, we have an orderbook that might be invalid
func (c *KrakenClient) verifyChecksum(ob *orderbook.OrderBook, receivedChecksum int64) bool {
	bids, asks := ob.GetTopLevels()
	checksumString := c.generateChecksumString(bids, asks)
	calculatedChecksum := int64(crc32.ChecksumIEEE([]byte(checksumString)))
	return calculatedChecksum == receivedChecksum
}

func (c *KrakenClient) generateChecksumString(bids, asks []orderbook.PriceLevel) string {
	var sb strings.Builder
	for _, ask := range asks {
		sb.WriteString(c.formatPriceLevel(ask.PriceStr, ask.AmountStr))
	}
	for _, bid := range bids {
		sb.WriteString(c.formatPriceLevel(bid.PriceStr, bid.AmountStr))
	}
	return sb.String()
}

func (c *KrakenClient) formatPriceLevel(priceStr, amountStr string) string {
	priceStr = strings.Replace(priceStr, ".", "", 1)
	amountStr = strings.Replace(amountStr, ".", "", 1)
	priceStr = strings.TrimLeft(priceStr, "0")
	amountStr = strings.TrimLeft(amountStr, "0")
	return priceStr + amountStr
}

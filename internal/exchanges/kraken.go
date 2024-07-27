package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"

	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const (
	// https://docs.kraken.com/api/docs/websocket-v2/book/
	// https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/
	krakenWSURL = "wss://ws.kraken.com/v2"
)

// Should fit the different messages sent
type krakenMessage struct {
	Channel string                `json:"channel,omitempty"`
	Data    json.RawMessage       `json:"data,omitempty"`
	Error   string                `json:"error,omitempty"`
	Method  string                `json:"method,omitempty"`
	Result  krakenSubscriptionAck `json:"result,omitempty"`
	Success *bool                 `json:"success,omitempty"`
	Type    string                `json:"type,omitempty"`
}

type krakenBookData struct {
	Asks      []priceQty `json:"asks,omitempty"`
	Bids      []priceQty `json:"bids,omitempty"`
	Checksum  int64      `json:"checksum"`
	Symbol    string     `json:"symbol"`
	Timestamp string     `json:"timestamp,omitempty"`
}

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
	subscription := krakenSubscriptionRequest{
		Method: "subscribe",
		Params: krakenSubscriptionParams{
			Channel: "book",
			Symbol:  pairs,
		},
	}
	log.Debug().Str("exchange", c.name).Interface("subscription", subscription).Msg("Sending subscription request")
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

			var msg krakenMessage
			if err := json.Unmarshal(message, &msg); err != nil {
				log.Error().Err(err).Str("exchange", c.name).Str("rawMessage", string(message)).Msg("Error unmarshalling message")
				continue
			}

			switch {
			case msg.Error != "":
				log.Error().Str("error", msg.Error).Msg("Could not subscribe")
			case msg.Channel == "book":
				var updates []krakenBookData
				if err := json.Unmarshal(msg.Data, &updates); err != nil {
					log.Error().Err(err).Str("exchange", c.name).Str("msgData", string(msg.Data)).Msg("Error unmarshalling book data")
					continue
				}
				for _, update := range updates {
					c.applyBookUpdate(update)
				}
			case msg.Channel == "status":
				log.Debug().Str("exchange", c.name).Msg("System status update")
			case msg.Channel == "heartbeat":
				log.Debug().Str("exchange", c.name).Msg("Heartbeat received")
			case msg.Method == "subscribe" && *msg.Success:
				log.Info().Str("exchange", c.name).Str("channel", msg.Result.Channel).Str("symbol", msg.Result.Symbol).Msg("Subscription successful")
			default:
				log.Warn().Str("exchange", c.name).Str("rawMessage", string(message)).Msg("Unhandled message type")
			}
		}
	}
}

func (c *KrakenClient) applyBookUpdate(data krakenBookData) {

	// Check that we can apply the message
	c.mu.RLock()
	ob, exists := c.orderBooks[data.Symbol]
	c.mu.RUnlock()
	if !exists {
		log.Warn().Str("exchange", c.name).Str("pair", data.Symbol).Msg("Order book not found for pair")
		return
	}

	// Send the Asks and Bids updates
	c.sendOrderBookUpdates(ob, data.Asks, "ask")
	c.sendOrderBookUpdates(ob, data.Bids, "bid")

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete

	if !c.verifyChecksum(ob, data.Checksum) {
		log.Error().Str("exchange", c.name).Str("pair", data.Symbol).Msg("Checksum verification failed")
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

		log.Debug().
			Str("exchange", c.name).
			Str("type", updateType).
			Str("price", update.PriceStr).
			Str("amount", update.QtyStr).
			Msg("Sending order book update")
	}
}

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

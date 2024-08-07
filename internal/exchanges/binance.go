package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dorskfr/arbichan/internal/messagetracker"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const (
	binanceAPIRestURL  = "https://api.binance.com/api/v3"
	binanceStreamWSURL = "wss://stream.binance.com:9443/ws"
)

type BinanceClient struct {
	name           string
	conn           *websocket.Conn
	orderBooks     map[string]*orderbook.OrderBook
	messagetracker *messagetracker.MessageTracker
	mu             sync.RWMutex
	lastUpdateIDs  map[string]int64
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
		name:           "binance",
		orderBooks:     make(map[string]*orderbook.OrderBook),
		lastUpdateIDs:  make(map[string]int64),
		messagetracker: messagetracker.NewMessageTracker("binance", time.Minute),
	}
}

func (c *BinanceClient) Name() string {
	return c.name
}

func (c *BinanceClient) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(binanceStreamWSURL, nil)
	if err != nil {
		return fmt.Errorf("error connecting to Binance Stream WebSocket: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *BinanceClient) Disconnect() {
	log.Info().Str("exchange", c.name).Msg("Disconnecting")
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *BinanceClient) RegisterOrderBook(symbol string, ob *orderbook.OrderBook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orderBooks[symbol] = ob
	log.Info().Str("exchange", c.name).Str("symbol", symbol).Msg("Registering orderbook")
}

func (c *BinanceClient) GetOrderBook(symbol string) *orderbook.OrderBook {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.orderBooks[symbol]
}

func (c *BinanceClient) Subscribe(symbols []string) error {
	streamParams := make([]string, len(symbols))
	for i, symbol := range symbols {
		streamParams[i] = fmt.Sprintf("%s@depth@100ms", strings.ToLower(symbol))
	}

	subscribeReq := binanceWSRequest{
		ID:     time.Now().Nanosecond(),
		Method: "SUBSCRIBE",
		Params: streamParams,
	}

	if err := c.conn.WriteJSON(subscribeReq); err != nil {
		return fmt.Errorf("error subscribing to depth streams: %w", err)
	}

	// Wait for subscription confirmation
	_, msg, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("error reading subscription response: %w", err)
	}

	var response binanceWSResponse
	if err := json.Unmarshal(msg, &response); err != nil {
		return fmt.Errorf("error unmarshalling subscription response: %w", err)
	}

	// On succesful subscription we just receive a `null` message with the ID of the request
	if response.ID == subscribeReq.ID {
		log.Info().Str("exchange", c.name).Msg("Successfully subscribed to depth streams")
	} else {
		return fmt.Errorf("unexpected subscription response for %s", response.Result)
	}

	// Get initial order book snapshots
	for _, symbol := range symbols {
		if err := c.getDepthSnapshot(symbol); err != nil {
			return fmt.Errorf("error getting depth snapshot for %s: %w", symbol, err)
		}
	}

	return nil
}

// Main function to read messages
func (c *BinanceClient) ReadMessages(ctx context.Context) error {
	messagetrackerTicker := time.NewTicker(time.Minute)
	defer messagetrackerTicker.Stop()

	for {
		select {
		// Stop when context is cancelled
		case <-ctx.Done():
			return ctx.Err()
		// Check if the last message received is not too old
		case <-messagetrackerTicker.C:
			c.messagetracker.CheckStaleConnection()
		// Otherwise process
		default:
			messageType, message, err := c.conn.ReadMessage()
			if err != nil {
				return fmt.Errorf("error reading message: %w", err)
			}

			// Refresh the messagetracker last message time
			c.messagetracker.RecordMessage()

			switch messageType {
			case websocket.PingMessage:
				// Binance pings every 3 min, respond to ping with a pong
				if err := c.conn.WriteMessage(websocket.PongMessage, message); err != nil {
					log.Warn().Err(err).Str("exchange", c.name).Msg("Failed to send pong")
					return fmt.Errorf("error sending pong: %w", err)
				}
				log.Debug().Str("exchange", c.name).Msg("Received ping, sent pong")
			case websocket.TextMessage:
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
			default:
				log.Warn().Str("exchange", c.name).Int("messageType", messageType).Msg("Received unexpected message type")
			}
		}
	}
}

func (c *BinanceClient) getDepthSnapshot(symbol string) error {
	url := fmt.Sprintf("%s/depth?symbol=%s&limit=10", binanceAPIRestURL, symbol)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error requesting depth snapshot: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading depth snapshot response: %w", err)
	}

	var snapshot binanceDepthSnapshot
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("error unmarshalling depth snapshot data: %w", err)
	}

	c.handleDepthSnapshot(symbol, &snapshot)
	return nil
}

func (c *BinanceClient) handleDepthSnapshot(symbol string, snapshot *binanceDepthSnapshot) {
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
	c.applyDepthUpdate(ob, snapshot.Bids, snapshot.Asks)

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete

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

	if update.FirstUpdateID > lastUpdateID+1 {
		// We've missed some updates, need to resync
		log.Warn().Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Missed updates, resyncing")
		if err := c.getDepthSnapshot(update.Symbol); err != nil {
			log.Error().Err(err).Str("exchange", c.name).Str("symbol", update.Symbol).Msg("Error resyncing")
		}
		return
	}

	if update.FinalUpdateID <= lastUpdateID {
		// Discard this update as it's older than what we have
		return
	}

	// Apply the update
	c.applyDepthUpdate(ob, update.Bids, update.Asks)

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete

	c.lastUpdateIDs[update.Symbol] = update.FinalUpdateID

}

func (c *BinanceClient) applyDepthUpdate(ob *orderbook.OrderBook, bids [][]string, asks [][]string) {
	for _, bid := range bids {
		price, _ := decimal.NewFromString(bid[0])
		amount, _ := decimal.NewFromString(bid[1])
		priceLevel := orderbook.PriceLevel{
			Type:      "bid",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
		ob.Updates <- priceLevel
	}
	for _, ask := range asks {
		price, _ := decimal.NewFromString(ask[0])
		amount, _ := decimal.NewFromString(ask[1])
		priceLevel := orderbook.PriceLevel{
			Type:      "ask",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
		ob.Updates <- priceLevel
	}
}

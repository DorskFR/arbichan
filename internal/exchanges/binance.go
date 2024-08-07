package exchanges

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dorskfr/arbichan/internal/messagetracker"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

const binanceAPIRestURL = "https://api.binance.com/api/v3"

type BinanceClient struct {
	*BaseExchangeClient
	messagetracker *messagetracker.MessageTracker
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
		BaseExchangeClient: NewBaseExchangeClient("binance", "wss://stream.binance.com:9443/ws"),
		lastUpdateIDs:      make(map[string]int64),
		messagetracker:     messagetracker.NewMessageTracker("binance", time.Minute),
	}
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

func (c *BinanceClient) ReadMessages(ctx context.Context) error {
	return c.BaseExchangeClient.ReadMessages(ctx, c.handleMessage)
}

func (c *BinanceClient) handleMessage(message []byte) error {
	var update binanceDepthUpdate
	if err := json.Unmarshal(message, &update); err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	if update.Symbol == "" {
		return fmt.Errorf("the message was not parsed correctly")
	}

	c.handleDepthUpdate(update)
	return nil
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

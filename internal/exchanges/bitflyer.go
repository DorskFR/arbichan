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

const bitflyerAPIRestURL = "https://api.bitflyer.com/v1/"

type BitflyerClient struct {
	*BaseExchangeClient
	messagetracker *messagetracker.MessageTracker
}

type bitflyerSubscribeRequest struct {
	JsonRpc string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  bitflyerParams `json:"params"`
	Id      int            `json:"id"`
}

type bitflyerParams struct {
	Channel string `json:"channel"`
}

type bitflyerWSResponse struct {
	JsonRpc string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	Params  bitflyerChannelMessage `json:"params"`
}

type bitflyerChannelMessage struct {
	Channel string                  `json:"channel"`
	Message bitflyerOrderBookUpdate `json:"message"`
}

type bitflyerOrderBookUpdate struct {
	MidPrice json.Number `json:"mid_price"`
	Bids     []price     `json:"bids"`
	Asks     []price     `json:"asks"`
}

type price struct {
	Price json.Number `json:"price"`
	Size  json.Number `json:"size"`
}

func NewBitflyerClient() *BitflyerClient {
	bitflyerClient := &BitflyerClient{messagetracker: messagetracker.NewMessageTracker("bitflyer", time.Minute)}
	bitflyerClient.BaseExchangeClient = NewBaseExchangeClient("bitflyer", "wss://ws.lightstream.bitflyer.com/json-rpc", bitflyerClient)
	return bitflyerClient
}

func (c *BitflyerClient) Subscribe(symbols []string) error {
	for _, symbol := range symbols {
		subscribeReq := bitflyerSubscribeRequest{
			JsonRpc: "2.0",
			Method:  "subscribe",
			Params: bitflyerParams{
				Channel: fmt.Sprintf("lightning_board_%s", symbol),
			},
			Id: 1,
		}
		if err := c.conn.WriteJSON(subscribeReq); err != nil {
			return fmt.Errorf("error subscribing to order book for %s: %w", symbol, err)
		}
		log.Info().Str("exchange", c.Name()).Str("symbol", symbol).Msg("Subscribed to order book")
		c.getOrderBookSnapshot(symbol)
	}
	return nil
}

func (c *BitflyerClient) ReadMessages(ctx context.Context) error {
	return c.BaseExchangeClient.ReadMessages(ctx, c.handleMessage, time.Minute)
}

func (c *BitflyerClient) handleMessage(message WebSocketMessage) error {
	var response bitflyerWSResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return fmt.Errorf("error unmarshalling message: %w", err)
	}

	if response.Method == "channelMessage" {
		symbol := strings.TrimPrefix(response.Params.Channel, "lightning_board_")
		c.handleOrderBookUpdate(symbol, response.Params.Message)
	}
	return nil
}

func (c *BitflyerClient) getOrderBookSnapshot(symbol string) error {
	url := fmt.Sprintf("%s/board?product_code=%s", bitflyerAPIRestURL, symbol)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error requesting depth snapshot: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading depth snapshot response: %w", err)
	}

	var snapshot bitflyerOrderBookUpdate
	if err := json.Unmarshal(body, &snapshot); err != nil {
		return fmt.Errorf("error unmarshalling depth snapshot data: %w", err)
	}

	c.handleOrderBookUpdate(symbol, snapshot)
	return nil
}

func (c *BitflyerClient) handleOrderBookUpdate(symbol string, update bitflyerOrderBookUpdate) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ob, exists := c.orderBooks[symbol]
	if !exists {
		log.Warn().Str("exchange", c.Name()).Str("symbol", symbol).Msg("Order book not found for symbol")
		return
	}

	c.applyOrderBookUpdate(ob, update)

	// Ensure all updates are processed
	ob.ProcessingComplete <- struct{}{}
	<-ob.ProcessingComplete
}

func (c *BitflyerClient) applyOrderBookUpdate(ob *orderbook.OrderBook, update bitflyerOrderBookUpdate) {
	for _, bid := range update.Bids {
		price, _ := decimal.NewFromString(bid.Price.String())
		amount, _ := decimal.NewFromString(bid.Size.String())
		ob.Updates <- orderbook.PriceLevel{
			Type:      "bid",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}

	for _, ask := range update.Asks {
		price, _ := decimal.NewFromString(ask.Price.String())
		amount, _ := decimal.NewFromString(ask.Size.String())
		ob.Updates <- orderbook.PriceLevel{
			Type:      "ask",
			Price:     price,
			Amount:    amount,
			PriceStr:  price.String(),
			AmountStr: amount.String(),
		}
	}
}

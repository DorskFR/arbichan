package exchanges

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dorskfr/arbichan/internal/messagetracker"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

type Pair struct {
	StandardSymbol  string
	ExchangePairs   []ExchangePair
	ProfitThreshold decimal.Decimal
}

type ExchangePair struct {
	Exchange string
	Symbol   string
}

type ExchangeClient interface {
	Name() string
	Connect(ctx context.Context) error
	Disconnect()
	RegisterOrderBook(symbol string, ob *orderbook.OrderBook)
	GetOrderBook(symbol string) *orderbook.OrderBook
	Subscribe(pairs []string) error
	ReadMessages(ctx context.Context) error
}

type BaseExchange struct {
}

type BaseExchangeClient struct {
	name           string
	url            string
	conn           *websocket.Conn
	orderBooks     map[string]*orderbook.OrderBook
	mu             sync.RWMutex
	messagetracker messagetracker.MessageTracker
}

func NewBaseExchangeClient(name string, url string) *BaseExchangeClient {
	return &BaseExchangeClient{
		name:       name,
		url:        url,
		orderBooks: make(map[string]*orderbook.OrderBook),
	}
}

func (c *BaseExchangeClient) Name() string {
	return c.name
}

func (c *BaseExchangeClient) RegisterOrderBook(symbol string, ob *orderbook.OrderBook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.orderBooks[symbol] = ob
	log.Info().Str("exchange", c.name).Str("symbol", symbol).Msg("Registering orderbook")
}

func (c *BaseExchangeClient) GetOrderBook(symbol string) *orderbook.OrderBook {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.orderBooks[symbol]
}

func (c *BaseExchangeClient) Connect(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("error connecting to Kraken WebSocket: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *BaseExchangeClient) Disconnect() {
	log.Info().Str("exchange", c.name).Msg("Disconnecting")
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *BaseExchangeClient) ReadMessages(ctx context.Context, handleMessage func([]byte) error) error {
	messagetrackerTicker := time.NewTicker(15 * time.Minute)
	defer messagetrackerTicker.Stop()

	pingTicker := time.NewTicker(time.Minute)
	defer pingTicker.Stop()

	readChan := make(chan []byte)
	errChan := make(chan error)

	go func() {
		for {
			_, message, err := c.conn.ReadMessage()
			if err != nil {
				errChan <- err
				return
			}
			select {
			case readChan <- message:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-messagetrackerTicker.C:
			c.messagetracker.CheckStaleConnection()
		case <-pingTicker.C:
			if err := c.sendPing(); err != nil {
				return err
			}
		case err := <-errChan:
			return fmt.Errorf("error reading message: %w", err)
		case message := <-readChan:
			c.messagetracker.RecordMessage()
			if err := handleMessage(message); err != nil {
				log.Error().Err(err).Str("exchange", c.Name()).Msg("Error handling message")
			}
		}
	}
}

func (c *BaseExchangeClient) sendPing() error {
	if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		log.Warn().Err(err).Str("exchange", c.Name()).Msg("Failed to send ping")
		return fmt.Errorf("error sending ping: %w", err)
	}
	return nil
}

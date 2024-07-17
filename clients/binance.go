package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"arbichan/api"

	"golang.org/x/time/rate"
)

type BinanceClient struct {
	client      *http.Client
	baseURL     string
	rateLimiter *rate.Limiter
}

type BinanceResponse struct {
	Asks [][]interface{} `json:"asks"`
	Bids [][]interface{} `json:"bids"`
}

func NewBinanceClient(client *http.Client, rateLimiter *rate.Limiter) *BinanceClient {
	return &BinanceClient{
		client:      client,
		baseURL:     "https://api.binance.com/api/v3/depth?limit=1",
		rateLimiter: rateLimiter,
	}
}

func (c *BinanceClient) GetName() string {
	return "binance"
}

func (c *BinanceClient) handleBinanceResponse(response *http.Response) (api.OrderDataPoint, api.OrderDataPoint, error) {
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error reading response body: %w", err)
	}

	var binanceResp BinanceResponse
	err = json.Unmarshal(body, &binanceResp)
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	if len(binanceResp.Asks) < 1 || len(binanceResp.Bids) < 1 {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling bid and ask: %w", err)
	}

	ask, err := c.parseOrderDataPoint("ask", binanceResp.Asks[0])
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling ask: %w", err)
	}

	bid, err := c.parseOrderDataPoint("bid", binanceResp.Bids[0])
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling bid: %w", err)
	}

	return ask, bid, nil
}

func (c *BinanceClient) parseOrderDataPoint(orderType string, data []interface{}) (api.OrderDataPoint, error) {
	if len(data) != 2 {
		return api.OrderDataPoint{}, fmt.Errorf("invalid data point length")
	}
	price, err := strconv.ParseFloat(data[0].(string), 64)
	if err != nil {
		return api.OrderDataPoint{}, fmt.Errorf("error parsing price: %w", err)
	}
	volume, err := strconv.ParseFloat(data[1].(string), 64)
	if err != nil {
		return api.OrderDataPoint{}, fmt.Errorf("error parsing volume: %w", err)
	}
	return api.OrderDataPoint{
		OrderType: orderType,
		Price:     price,
		Volume:    volume,
		OrderTime: time.Now(),
	}, nil
}

func (c *BinanceClient) GetAskAndBid(ctx context.Context, pair string) (api.ExchangeAskBid, error) {

	if err := c.rateLimiter.Wait(ctx); err != nil {
		return api.ExchangeAskBid{}, fmt.Errorf("rate limit error: %w", err)
	}

	url := fmt.Sprintf("%s&symbol=%s", c.baseURL, pair)
	response, err := api.Get(ctx, c.client, url)
	if err != nil {
		return api.ExchangeAskBid{Name: "binance", Ask: api.OrderDataPoint{}, Bid: api.OrderDataPoint{}}, err
	}

	ask, bid, err := c.handleBinanceResponse(response)
	if err != nil {
		return api.ExchangeAskBid{Name: "binance", Ask: ask, Bid: bid}, err
	}

	return api.ExchangeAskBid{Name: "binance", Ask: ask, Bid: bid}, nil
}

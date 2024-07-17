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

type KrakenClient struct {
	client      *http.Client
	baseURL     string
	rateLimiter *rate.Limiter
}

type krakenResponse struct {
	Error  []string `json:"error"`
	Result map[string]struct {
		Asks [][]interface{} `json:"asks"`
		Bids [][]interface{} `json:"bids"`
	} `json:"result"`
}

func NewKrakenClient(client *http.Client, rateLimiter *rate.Limiter) *KrakenClient {
	return &KrakenClient{
		client:      client,
		baseURL:     "https://api.kraken.com/0/public/Depth?count=1",
		rateLimiter: rateLimiter,
	}
}

func (c *KrakenClient) GetName() string {
	return "kraken"
}

func (c *KrakenClient) handleKrakenResponse(response *http.Response, pair string) (api.OrderDataPoint, api.OrderDataPoint, error) {
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error reading response body: %w", err)
	}

	var krakenResp krakenResponse
	err = json.Unmarshal(body, &krakenResp)
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	if len(krakenResp.Result[pair].Asks) < 1 || len(krakenResp.Result[pair].Bids) < 1 {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling bid and ask: %w", err)
	}

	ask, err := c.parseOrderDataPoint("ask", krakenResp.Result[pair].Asks[0])
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling ask: %w", err)
	}

	bid, err := c.parseOrderDataPoint("bid", krakenResp.Result[pair].Bids[0])
	if err != nil {
		return api.OrderDataPoint{}, api.OrderDataPoint{}, fmt.Errorf("error unmarshaling bid: %w", err)
	}

	return ask, bid, nil
}

func (c *KrakenClient) parseOrderDataPoint(orderType string, data []interface{}) (api.OrderDataPoint, error) {
	if len(data) != 3 {
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
		OrderTime: time.Unix(int64(data[2].(float64)), 0),
	}, nil
}

func (c *KrakenClient) GetAskAndBid(ctx context.Context, pair string) (api.ExchangeAskBid, error) {
	if err := c.rateLimiter.Wait(ctx); err != nil {
		return api.ExchangeAskBid{}, fmt.Errorf("rate limit error: %w", err)
	}

	url := fmt.Sprintf("%s&pair=%s", c.baseURL, pair)
	response, err := api.Get(ctx, c.client, url)
	if err != nil {
		return api.ExchangeAskBid{Name: "kraken", Ask: api.OrderDataPoint{}, Bid: api.OrderDataPoint{}}, err
	}

	ask, bid, err := c.handleKrakenResponse(response, pair)
	if err != nil {
		return api.ExchangeAskBid{Name: "kraken", Ask: ask, Bid: bid}, err
	}

	return api.ExchangeAskBid{Name: "kraken", Ask: ask, Bid: bid}, nil
}

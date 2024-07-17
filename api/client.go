package api

import (
	"context"
	"fmt"
	"net/http"
)

type Client interface {
	GetName() string
	GetAskAndBid(ctx context.Context, pair string) (ExchangeAskBid, error)
}

func Get(ctx context.Context, client *http.Client, url string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("there was an error %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("there was an error %w", err)
	}
	return response, nil
}

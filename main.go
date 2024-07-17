package main

import (
	"arbichan/api"
	"arbichan/clients"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"net/http"
	"time"
)

type NetworkError struct {
	Err error
}

func (e NetworkError) Error() string {
	return fmt.Sprintf("network error: %v", e.Err)
}

type APIError struct {
	StatusCode int
	Message    string
}

func (e APIError) Error() string {
	return fmt.Sprintf("API error (status %d): %s", e.StatusCode, e.Message)
}

// TODO:
// A simple HTTP call to notify via signal, mattermost, etc.
// Optional: a database to save history
func main() {
	// Setup zerolog
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.TimeFieldFormat = time.RFC3339
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05",
	})

	// Graceful Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info().Msg("Received shutdown signal. Cancelling operations...")
		cancel()
	}()

	// A timer that ticks every N interval
	var interval = 5 * time.Second
	var timeout = interval - 50*time.Millisecond
	var ticker = time.NewTicker(interval)
	defer ticker.Stop()

	// HTTP Client Configuration
	httpClient := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Clients and pairs
	// Each client has its own rate limit
	exchangeClients := map[string]api.Client{
		"kraken":  clients.NewKrakenClient(httpClient, rate.NewLimiter(rate.Every(2*time.Second), 1)),
		"binance": clients.NewBinanceClient(httpClient, rate.NewLimiter(rate.Every(time.Second/8), 1)),
	}

	// Hardcoded pairs to move to a db or config file
	var pairs = map[string][]api.ExchangePair{
		"ETHEUR": {{Exchange: "kraken", Pair: "XETHZEUR"}, {Exchange: "binance", Pair: "ETHEUR"}},
		"BTCEUR": {{Exchange: "kraken", Pair: "XXBTZEUR"}, {Exchange: "binance", Pair: "BTCEUR"}},
	}

	// Main loop
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Shutting down...")
			return
		case <-ticker.C:
			processAllPairs(ctx, exchangeClients, pairs, timeout)
		}
	}
}

// Method that spawns each pair's goroutine (fetch and compare) and awaits the result
func processAllPairs(ctx context.Context, exchangeClients map[string]api.Client, pairs map[string][]api.ExchangePair, timeout time.Duration) {
	var wg sync.WaitGroup
	var pairTimeout = timeout - 50*time.Millisecond
	pairCtx, cancel := context.WithTimeout(ctx, pairTimeout)
	defer cancel()

	for pair, exchangePairs := range pairs {
		wg.Add(1)
		go func(pair string, exchangePairs []api.ExchangePair) {
			defer wg.Done()
			processExchangePair(pairCtx, exchangeClients, pair, exchangePairs, pairTimeout)
		}(pair, exchangePairs)
	}

	wg.Wait()
}

// For each pair spawn as many goroutines as there are exchangePairs to fetch
// Then compare the results to find arbitrage opportunities
func processExchangePair(
	ctx context.Context,
	exchangeClients map[string]api.Client,
	pair string,
	exchangePairs []api.ExchangePair,
	timeout time.Duration,
) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout-50*time.Millisecond)
	defer cancel()

	results := make(chan api.ExchangeAskBid, len(exchangePairs))
	var wg sync.WaitGroup

	for _, ep := range exchangePairs {
		wg.Add(1)
		go func(ep api.ExchangePair) {
			defer wg.Done()
			askAndBid, err := getExchangeData(timeoutCtx, exchangeClients[ep.Exchange], ep.Pair)
			if err != nil {
				log.Error().
					Err(err).
					Str("exchange", ep.Exchange).
					Str("pair", ep.Pair).
					Msg("Failed to get exchange data")
				return
			}
			select {
			case results <- askAndBid:
			case <-timeoutCtx.Done():
				// Context cancelled, discard result
			}
		}(ep)
	}

	// Close the results channel when all goroutines are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var validResults []api.ExchangeAskBid
	for result := range results {
		validResults = append(validResults, result)
	}

	if len(validResults) >= len(exchangePairs)-1 {
		pairwise(pair, validResults)
	} else {
		log.Warn().
			Str("pair", pair).
			Int("expected", len(exchangePairs)).
			Int("received", len(validResults)).
			Msg("Insufficient data to calculate arbitrage")
	}
}

func getExchangeData(ctx context.Context, client api.Client, pair string) (api.ExchangeAskBid, error) {
	result, err := client.GetAskAndBid(ctx, pair)
	if err != nil {
		return api.ExchangeAskBid{}, fmt.Errorf("failed to get ask and bid: %w", err)
	}
	return result, nil
}

func CalculateProfit(buyPrice float64, sellPrice float64, volume float64) float64 {
	return (buyPrice - sellPrice) * volume
}

func pairwise(pair string, results []api.ExchangeAskBid) {
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			PrintArbitrageOpportunity(pair, results[i], results[j])
		}
	}
}

func PrintArbitrageOpportunity(pair string, exchange1 api.ExchangeAskBid, exchange2 api.ExchangeAskBid) {
	if exchange1.Bid.Price > exchange2.Ask.Price {
		profit := CalculateProfit(exchange1.Bid.Price, exchange2.Ask.Price, min(exchange1.Bid.Volume, exchange2.Ask.Volume))
		percentage := profit / exchange2.Ask.Price * 100
		percentageNet := percentage - 0.5
		if percentageNet > 0 {
			log.Info().Msgf(
				"[%s] %.6f pct (%.2f) arbitrage opportunity buying from %s and selling to %s",
				pair,
				percentage,
				profit,
				exchange2.Name,
				exchange1.Name,
			)
			return
		}
	} else if exchange2.Bid.Price > exchange1.Ask.Price {
		profit := CalculateProfit(exchange2.Bid.Price, exchange1.Ask.Price, min(exchange1.Bid.Volume, exchange2.Ask.Volume))
		percentage := profit / exchange1.Ask.Price * 100
		percentageNet := percentage - 0.5
		if percentageNet > 0 {
			log.Info().Msgf(
				"[%s] %.6f pct (%.2f) arbitrage opportunity buying from %s and selling to %s",
				pair,
				percentage,
				profit,
				exchange1.Name,
				exchange2.Name,
			)
			return
		}
	}
	log.Info().Msgf("[%s] No arbitrage opportunity", pair)
}

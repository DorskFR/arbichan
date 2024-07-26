package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/dorskfr/arbichan/internal/arbitrage"
	"github.com/dorskfr/arbichan/internal/exchanges"
	"github.com/dorskfr/arbichan/internal/orderbook"
	"github.com/dorskfr/arbichan/internal/utils"
	"github.com/rs/zerolog/log"
)

func main() {
	// Initialize logging
	utils.InitLogging()

	// Define the pairs we want to compare
	pairs := []exchanges.Pair{
		{
			StandardSymbol: "BTC-USD",
			ExchangePairs: []exchanges.ExchangePair{
				{Exchange: "binance", Symbol: "BTCUSD"},
				{Exchange: "kraken", Symbol: "XBT/USD"},
			},
		},
		// Add more pairs here as needed
	}

	// Create channels for updates
	majorUpdateChan := make(chan orderbook.MajorUpdate, 100)

	// Create arbitrage detector
	detector := arbitrage.NewArbitrageDetector(pairs, majorUpdateChan)

	// Create exchange clients
	exchangeClients := map[string]exchanges.ExchangeClient{
		"binance": exchanges.NewBinanceClient(),
		"kraken":  exchanges.NewKrakenClient(),
	}

	// Create order books
	orderBooks := make(map[string]*orderbook.OrderBook)

	// Initialize order books and register them with clients and detector
	for _, pair := range pairs {
		for _, ep := range pair.ExchangePairs {
			key := fmt.Sprintf("%s:%s", ep.Exchange, ep.Symbol)
			updateChan := make(chan orderbook.OrderBookUpdate, 100)
			ob := orderbook.NewOrderBook(ep.Exchange, ep.Symbol, updateChan, majorUpdateChan)
			orderBooks[key] = ob
			exchangeClients[ep.Exchange].RegisterOrderBook(ep.Symbol, ob)
			detector.RegisterOrderBook(ep.Exchange, ep.Symbol, ob)
		}
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start order book processors
	for _, ob := range orderBooks {
		go ob.Run(ctx)
	}

	// Start arbitrage detector
	go detector.Run(ctx)

	// Connect to exchanges and start reading messages
	var wg sync.WaitGroup
	for name, client := range exchangeClients {
		wg.Add(1)
		symbols := getSymbolsForExchange(name, pairs)
		go runExchangeClient(ctx, &wg, client, symbols)
	}

	// Wait for shutdown signal
	utils.WaitForShutdownSignal(cancel)
	wg.Wait()
	log.Info().Msg("Shutdown complete")
}

func runExchangeClient(ctx context.Context, wg *sync.WaitGroup, client exchanges.ExchangeClient, symbols []string) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Info().Str("exchange", client.Name()).Msg("Context cancelled, shutting down client")
			client.Disconnect()
			return
		default:
			log.Info().Str("exchange", client.Name()).Msg("Attempting to connect")
			if err := client.Connect(); err != nil {
				log.Error().Err(err).Str("exchange", client.Name()).Msg("Failed to connect")
				continue
			}

			log.Info().Str("exchange", client.Name()).Msg("Connected successfully, subscribing to symbols")
			if err := client.Subscribe(symbols); err != nil {
				log.Error().Err(err).Str("exchange", client.Name()).Msg("Failed to subscribe")
				client.Disconnect()
				continue
			}

			log.Info().Str("exchange", client.Name()).Msg("Subscribed successfully, starting to read messages")
			if err := client.ReadMessages(ctx); err != nil {
				log.Error().Err(err).Str("exchange", client.Name()).Msg("Error reading messages")
				client.Disconnect()
			}
		}
	}
}

func getSymbolsForExchange(exchangeName string, pairs []exchanges.Pair) []string {
	var symbols []string
	for _, pair := range pairs {
		for _, ep := range pair.ExchangePairs {
			if ep.Exchange == exchangeName {
				symbols = append(symbols, ep.Symbol)
			}
		}
	}
	return symbols
}

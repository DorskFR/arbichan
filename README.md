# Arbitrage Opportunity Finder

This Go-based project monitors multiple cryptocurrency exchanges in real-time to detect arbitrage opportunities. Binance, Kraken, and Poloniex clients are available as an example.

It could be used as a base to develop a private arbitrage bot trader.

## Features

- Real-time order book tracking for multiple exchanges
- Configurable trading pairs and profit thresholds
- Concurrent processing of order book updates
- Logging of detected arbitrage opportunities

## Todo

- Automatic reconnection and resubscription on connection loss

## Installation

1. Clone the repository:

```bash
git clone https://github.com/DorskFR/arbichan.git
cd arbichan
```

2. Install dependencies:

```bash
go mod tidy
# or
make setup
```

## Configuration

The main configuration is done in the `main.go` file. You can modify the `pairs` slice to add or remove trading pairs and exchanges:

```go
pairs := []exchanges.Pair{
    {
        StandardSymbol: "BTC-USD",
        ExchangePairs: []exchanges.ExchangePair{
            {Exchange: "binance", Symbol: "BTCUSDT"},
            {Exchange: "kraken", Symbol: "BTC/USD"},
            {Exchange: "poloniex", Symbol: "BTC_USDT"},
        },
        ProfitThreshold: decimal.NewFromFloat32(0.50),
    },
    // Add more pairs here
}
```

## Usage

To run the arbitrage finder:

```bash
go run main.go
# or
make run
```

The program will connect to the configured exchanges, subscribe to the specified trading pairs, and start monitoring for arbitrage opportunities. Detected opportunities will be logged to the console.

## Project Structure

- `main.go`: Entry point of the application, sets up exchanges and starts the arbitrage detector
- `internal/`:
  - `arbitrage/`: Contains the arbitrage detection logic
  - `exchanges/`: Implements exchange-specific clients (Binance, Kraken, Poloniex)
  - `orderbook/`: Manages the order book data structure and updates
  - `utils/`: Utility functions for logging and graceful shutdown
- `messagetracker/`: Tracks message frequency to detect stale connections

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This software is for educational purposes only. Use it at your own risk. The authors and contributors are not responsible for any financial losses incurred through the use of this software.

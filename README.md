# Crypto Trading Bot

This project implements a simple trading bot for Coinbase Advanced using 15 minute candles. Candles, orders and tick logs are persisted in a local SQLite database so the bot can resume after restarts.

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root containing API credentials with **read** and **trade** permissions only:

```
COINBASE_API_KEY=your_key
COINBASE_API_SECRET=your_secret
COINBASE_API_PASSPHRASE=your_passphrase
```

## Running the Bot

```bash
python bot.py --live      # live trading (default)
python bot.py --paper     # disable order placement
```

Additional options:

- `--risk` risk per trade (default 0.01)
- `--loglevel` Python logging level

## Testing

Run the unit tests with:

```bash
python -m pytest -q
```

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider awesome \
  --symbol USD-BRL \
  --asset USD \
  --currency BRL

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider awesome \
  --symbol JPY-BRL \
  --asset JPY \
  --currency BRL

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider binance \
  --symbol BTCUSDT \
  --asset BTC \
  --currency USDT

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider binance \
  --symbol ETHUSDT \
  --asset ETH \
  --currency USDT

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider binance \
  --symbol SOLUSDT \
  --asset SOL \
  --currency USDT

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider yfinance \
  --symbol ^GSPC \
  --asset "S&P500" \
  --currency USD

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider yfinance \
  --symbol ^BVSP \
  --asset IBOV \
  --currency BRL

python -m src.data_ingestion.data_ingestion \
  --mode individual \
  --provider ipea \
  --symbol BM12_TJCDI12 \
  --asset CDI \
  --currency prc \
  --start_date "2000-01-01"

python -m src.data_ingestion.data_ingestion --mode brazil

python -m src.data_ingestion.binance_order_history

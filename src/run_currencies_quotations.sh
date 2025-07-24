python -m src.data_ingestion.data_ingestion --provider awesome --currency USD-BRL --table usdbrl
python -m src.data_ingestion.data_ingestion --provider awesome --currency JPY-BRL --table jpybrl
python -m src.data_ingestion.data_ingestion --provider binance --currency BTCUSDT --table btcusdt
python -m src.data_ingestion.data_ingestion --provider binance --currency ETHUSDT --table ethusdt
python -m src.data_ingestion.data_ingestion --provider yfinance --currency IVVB11.SA --table ivvb11brl
python -m src.data_ingestion.data_ingestion --provider yfinance --currency ^GSPC --table sp500usd
python -m src.data_ingestion.data_ingestion --provider yfinance --currency ^BVSP --table ibovbrl
python -m src.data_ingestion.data_ingestion --provider ipea --currency BM12_TJCDI12 --table cdi_prc --start_date 2000-01-01
python -m src.data_ingestion.binance_order_history

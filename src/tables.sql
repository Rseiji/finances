-- CURRENCIES
CREATE SCHEMA IF NOT EXISTS currencies;

CREATE TABLE currencies.quotations (
    date DATE NOT NULL,
    asset TEXT NOT NULL,
    currency TEXT NOT NULL,
    value FLOAT,
    _processed_at TIMESTAMP,
    PRIMARY KEY (date, asset, currency)
)


-- CRYPTO
CREATE SCHEMA crypto;

CREATE TABLE crypto.brl_deposits (
    id TEXT NOT NULL PRIMARY KEY,
    date DATE,
    value_brl FLOAT,
    exchange_name TEXT,
    _processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crypto.swaps (
    id TEXT NOT NULL PRIMARY KEY,
    date DATE,
    received_amount DOUBLE PRECISION NOT NULL,
    paid_taxes_amount DOUBLE PRECISION NOT NULL,
    paid_amount DOUBLE PRECISION NOT NULL,
    received_currency TEXT NOT NULL,
    paid_taxes_currency TEXT,
    paid_currency TEXT,
    exchange_name TEXT,
    _processed_at TIMESTAMP
);

CREATE TABLE crypto.earnings (
    id TEXT NOT NULL PRIMARY KEY,
    date DATE NOT NULL,
    currency TEXT NOT NULL,
    source TEXT NOT NULL,
    earning_amount DOUBLE PRECISION NOT NULL,
    _processed_at TIMESTAMP
);

CREATE TABLE crypto.withdraws (
    id TEXT NOT NULL PRIMARY KEY,
    date DATE NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    tax DOUBLE PRECISION NOT NULL,
    currency_amount TEXT NOT NULL,
    currency_tax TEXT NOT NULL,
    source TEXT NOT NULL,
    destiny TEXT NOT NULL,
    _processed_at TIMESTAMP
);

CREATE TABLE crypto.manually_inserted_keys (
    id TEXT NOT NULL,
    _processed_at TIMESTAMP
);

-- STOCKS
CREATE SCHEMA stocks;

CREATE TABLE stocks.transactions (
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    taxes DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    current_quantity DOUBLE PRECISION NOT NULL,
    _processed_at TIMESTAMP,
    PRIMARY KEY (date, ticker, quantity, price)
);

create table stocks.dividends_incomes (
	date DATE NOT NULL,
	ticker TEXT NOT NULL,
	type TEXT NOT NULL,
	source TEXT NOT NULL,
	value FLOAT,
	_processed_at TIMESTAMP,
	PRIMARY KEY (date, ticker, type, source)
)

create table stocks.allocations (
    year_month date,
    macroallocation text,
    value float,
    _processed_at TIMESTAMP,
    PRIMARY KEY (year_month, macroallocation)
)

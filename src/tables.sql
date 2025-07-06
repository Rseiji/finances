CREATE SCHEMA IF NOT EXISTS currencies;

CREATE TABLE currencies.usdbrl (
    date DATE,
    value FLOAT,
    _processed_at TIMESTAMP
);

CREATE TABLE currencies.jpybrl (
    date DATE,
    value FLOAT,
    _processed_at TIMESTAMP
);

CREATE TABLE currencies.btcbrl (
    date DATE,
    value FLOAT,
    _processed_at TIMESTAMP
);

CREATE TABLE currencies.ethbrl (
    date DATE,
    value FLOAT,
    _processed_at TIMESTAMP
);

CREATE SCHEMA crypto;

CREATE TABLE crypto.deposits (
    deposit_date DATE,
    value_brl FLOAT,
    exchange_name TEXT,
    _processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crypto.swaps (
    swap_date DATE,
    received_amount DOUBLE PRECISION NOT NULL,
    paid_taxes_amount DOUBLE PRECISION NOT NULL,
    paid_amount DOUBLE PRECISION NOT NULL,
    received_currency TEXT NOT NULL,
    paid_taxes_currency TEXT NOT NULL,
    paid_currency TEXT NOT NULL,
    exchange_name TEXT,
    _processed_at TIMESTAMP
);

CREATE TABLE crypto.earnings (
    earning_date DATE NOT NULL,
    earning_currency TEXT NOT NULL,
    source TEXT NOT NULL,
    earning_amount DOUBLE PRECISION NOT NULL,
    _processed_at TIMESTAMP
);

CREATE TABLE crypto.withdraws (
    withdraw_date DATE NOT NULL,
    amount DOUBLE PRECISION NOT NULL,
    tax DOUBLE PRECISION NOT NULL,
    currency_amount TEXT NOT NULL,
    currency_tax TEXT NOT NULL,
    source TEXT NOT NULL,
    destiny TEXT NOT NULL
);

CREATE SCHEMA stocks;

CREATE TABLE stocks.transactions (
    date DATE NOT NULL,
    ticker TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    taxes DOUBLE PRECISION NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    current_quantity DOUBLE PRECISION NOT NULL,
    _processed_at TIMESTAMP
);

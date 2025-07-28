"""Fetch and persist currency prices from APIs."""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
import psycopg2
import requests
from dateutil.relativedelta import relativedelta
from psycopg2 import OperationalError

from src.utils import persist_dataframe_to_database, read_sql_query

from .awesome_api import get_awesome_close_prices
from .binance_api import get_binance_close_prices
from .ipea_api import get_ipea_close_prices
from .yfinance_api import get_yfinance_close_prices

PROVIDERS = {
    "awesome": get_awesome_close_prices,
    "binance": get_binance_close_prices,
    "yfinance": get_yfinance_close_prices,
    "ipea": get_ipea_close_prices,
}

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


def get_currencies_data_from_last_record(
    fetch_function: callable,
    symbol: str,
    asset: str,
    currency: str,
    table_schema: str,
    table_name: str,
    start_date: str | None = None,
    **_: Any,
) -> pd.DataFrame:
    """Get currency data starting from the last record.

    Parameters
    ----------
    fetch_function : callable
        Function to fetch currency data.
    symbol : str
        Symbol to be passed to API. Check on the desired API's documentation about what to pass
        here.
    asset : str
        Asset we want to fetch. That's how it will be registered in the database.
    currency : str
        Asset's conversion currency, e.g. USD, BRL, etc.
    table_schema : str
        Schema name of the target database table.
    table_name : str
        Name of the target database table.
    start_date : str
        Date from which to start fetching data, in format "%Y-%m-%d". If not passed,

    Returns
    -------
    data : pd.DataFrame
        Fetched data.
    """
    if start_date:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        parsed_start_date = datetime.strftime(
            get_last_persisted_data(f"""{table_schema}.{table_name}""", asset, currency)
            + relativedelta(days=1), "%Y-%m-%d"
        )

    data = (
        fetch_function(
            symbol=symbol,
            start_date=parsed_start_date,
            end_date=datetime.now().strftime("%Y-%m-%d"),
        )
        .assign(asset=asset, currency=currency)
    )

    return data


def get_last_persisted_data(
    table_name: str, asset: str, currency: str, default=datetime(2020, 1, 1)
) -> datetime:
    """Retrieve the most recent date from the specified database table.

    Parameters
    ----------
    table_name : str
        Name of the table to query, including schema if needed.
    default : datetime, optional
        Default date to return if no data is found. Default is datetime(2020, 1, 1).

    Returns
    -------
    datetime
        The most recent date found in the table, or the default date if the table is empty.
    """
    max_min_ranges = read_sql_query(f"""
        SELECT MAX(date) AS max_date
        FROM {table_name}
        WHERE asset = '{asset}'
        AND currency = '{currency}'
    """)

    if max_min_ranges.empty or pd.isna(max_min_ranges["max_date"].iloc[0]):
        logging.info(
            f"No data found in {table_name}, returning default date {default}."
        )
        return default
    return max_min_ranges["max_date"].iloc[0]


def ingest_currency_data(data: pd.DataFrame, table_schema: str, table_name: str):
    if data is not None and not data.empty:
        persist_dataframe_to_database(
            data,
            schema=table_schema,
            table=table_name,
            assign_processed_at_column=True,
            pk_columns=["date", "asset", "currency"]
        )
    else:
        logging.warning("No data to persist. Skipping persistence.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and persist currency prices from AwesomeAPI."
    )
    parser.add_argument(
        "--provider", default="awesome", help="Data provider to use."
    )
    parser.add_argument(
        "--symbol", help="Symbol to fetch from API. Check on API docs to know what to put here."
    )
    parser.add_argument(
        "--asset", help="Asset to fetch from API. This is the code that will be stored in database."
    )
    parser.add_argument(
        "--currency", help="Currency to fetch from API, e.g. BRL, USD, etc."
    )
    parser.add_argument(
        "--start_date", default=None, help="Date to start parsing the time series."
    )
    args = parser.parse_args()

    table_schema = "currencies"
    table_name = "quotations"

    fetch_fn = PROVIDERS[args.provider]
    data = get_currencies_data_from_last_record(
        fetch_fn,
        args.symbol,
        args.asset,
        args.currency,
        table_schema,
        table_name,
        args.start_date,
    )
    ingest_currency_data(data, table_schema, table_name)

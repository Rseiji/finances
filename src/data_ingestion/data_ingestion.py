"""Fetch and persist currency prices from APIs."""

import requests
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
from src.utils import persist_dataframe_to_database, read_sql_query
import pandas as pd
from dateutil.relativedelta import relativedelta
import logging

from .awesome_api import get_awesome_close_prices
from .binance_api import get_binance_close_prices

PROVIDERS = {
    "awesome": get_awesome_close_prices,
    "binance": get_binance_close_prices,
}

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


def ingest_currencies_data_from_last_record(
    fetch_function: callable,
    symbol: str,
    table_schema: str,
    table_name: str,
) -> None:
    """Ingest currency data starting from the last record.

    Parameters
    ----------
    fetch_function : callable
        Function to fetch currency data.
    symbol : str
        Currency pair symbol to fetch.
    table_schema : str
        Schema name of the target database table.
    table_name : str
        Name of the target database table.

    Returns
    -------
    None
    """
    data = fetch_function(
        symbol=symbol,
        start_date=datetime.strftime(
            (
                get_last_persisted_data(f"{table_schema}.{table_name}")
                + relativedelta(days=1)
            ),
            "%Y-%m-%d",
        ),
        end_date=datetime.now().strftime("%Y-%m-%d"),
    )

    if data is not None and not data.empty:
        persist_dataframe_to_database(
            data,
            schema=table_schema,
            table=table_name,
            assign_processed_at_column=True,
            pk_columns=["date"]
        )
    else:
        logging.warning(
            "Data does not contain required columns 'bid' and 'timestamp'. Skipping persistence."
        )


def get_last_persisted_data(table_name: str, default=datetime(2020, 1, 1)) -> datetime:
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
    max_min_ranges = read_sql_query(f"select max(date) as max_date from {table_name}")
    if max_min_ranges.empty or pd.isna(max_min_ranges["max_date"].iloc[0]):
        logging.info(
            f"No data found in {table_name}, returning default date {default}."
        )
        return default
    return max_min_ranges["max_date"].iloc[0]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and persist currency prices from AwesomeAPI."
    )
    parser.add_argument(
        "--provider", choices=["awesome", "binance"], default="awesome", help="Data provider to use."
    )
    parser.add_argument(
        "--currency", default="USD-BRL:usdbrl", help="Currency pair symbol to fetch."
    )
    args = parser.parse_args()

    fetch_fn = PROVIDERS[args.provider]
    ingest_currencies_data_from_last_record(
        fetch_fn, args.currency, "currencies", args.currency.lower().replace("-", "")
    )

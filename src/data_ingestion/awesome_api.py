"""Fetch and persist currency prices from AwesomeAPI."""
import requests
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
from src.utils import persist_dataframe_to_database, read_sql_query
import pandas as pd
from dateutil.relativedelta import relativedelta
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')


def get_currency_price(
    currency_symbol: str, start_date: int | str, end_date: int | str
) -> pd.DataFrame | None:
    """Fetch historical currency price data from the AwesomeAPI.

    Args:
        currency_symbol (str): The currency pair symbol (e.g., "USD-BRL").
        start_date (int): The start date in YYYYMMDD format.
        end_date (int): The end date in YYYYMMDD format.

    Returns:
        pd.DataFrame or None: DataFrame with currency data if successful, otherwise None.
    """
    num_days = (
        datetime.strptime(str(end_date), "%Y%m%d")
        - datetime.strptime(str(start_date), "%Y%m%d")
    ).days

    url = (
        f"https://economia.awesomeapi.com.br/json/daily/{currency_symbol}/{num_days}/"
        f"?start_date={start_date}&end_date={end_date}"
    )

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        json_data = response.json()
        if isinstance(json_data, list) and len(json_data) > 0:
            df = pd.DataFrame(json_data)
            required_columns = {"bid", "timestamp"}
            if required_columns.issubset(df.columns):
                data = df.assign(
                    timestamp=lambda df: pd.to_datetime(df["timestamp"], unit="s"))
                return data
            else:
                missing = required_columns - set(df.columns)
                logging.warning(f"Missing columns in API response: {missing}")
    except Exception as e:
        logging.error(f"Error fetching currency price: {e}")
        return None


def get_last_persisted_data(table_name: str, default=datetime(2020, 1, 1)):
    """
    Retrieves the most recent date from the specified database table.

    Args:
        table_name (str): The name of the table to query, including schema if needed.
        default (datetime, optional): The default date to return if no data is found. Defaults to datetime(2020, 1, 1).

    Returns:
        datetime: The most recent date found in the table, or the default date if the table is empty.
    """
    max_min_ranges = read_sql_query(f"select max(date) as max_date from {table_name}")
    if max_min_ranges.empty or pd.isna(max_min_ranges["max_date"].iloc[0]):
        logging.info(
            f"No data found in {table_name}, returning default date {default}."
        )
        return default
    return max_min_ranges["max_date"].iloc[0]


def run(
    currency_symbol: str = "USD-BRL",
    table_schema: str = "currencies",
    table_name: str = "usdbrl",
):
    """Fetch the latest currency prices from the AwesomeAPI and persist it to database."""
    data = get_currency_price(
        currency_symbol=currency_symbol,
        start_date=datetime.strftime(
            (
                get_last_persisted_data(f"{table_schema}.{table_name}")
                + relativedelta(days=1)
            ),
            "%Y%m%d",
        ),
        end_date=datetime.now().strftime("%Y%m%d"),
    )

    if data is not None and {"bid", "timestamp"}.issubset(data.columns):
        persist_dataframe_to_database(
            (
                data.rename(columns={"bid": "value", "timestamp": "date"}).assign(
                    date=lambda df: df["date"].dt.date
                )
            )[["date", "value"]],
            schema=table_schema,
            table=table_name,
            assign_processed_at_column=True,
        )
    else:
        logging.warning(
            "Data does not contain required columns 'bid' and 'timestamp'. Skipping persistence."
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and persist currency prices from AwesomeAPI."
    )
    parser.add_argument(
        "--currency", default="USD-BRL:usdbrl", help="Currency pair symbol to fetch."
    )
    args = parser.parse_args()

    if "-" not in args.currency:
        raise ValueError("Currency must be in the format 'USD-BRL'.")

    run(args.currency, "currencies", args.currency.lower().replace("-", ""))

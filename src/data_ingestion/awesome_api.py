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


def get_awesome_close_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    **_: dict[str, str] | None
) -> pd.DataFrame | None:
    """
    Fetch historical currency price data from the AwesomeAPI.

    Parameters
    ----------
    symbol : str
        The currency pair symbol (e.g., "USD-BRL").
    start_date : str
        The start date in 'YYYY-MM-DD' format.
    end_date : str
        The end date in 'YYYY-MM-DD' format.

    Returns
    -------
    pd.DataFrame or None
        DataFrame with currency data if successful, otherwise None.
    """
    num_days = (
        datetime.strptime(end_date, "%Y-%m-%d")
        - datetime.strptime(start_date, "%Y-%m-%d")
    ).days

    url = (
        f"https://economia.awesomeapi.com.br/json/daily/{symbol}/{num_days}/"
        f"?start_date={start_date.replace("-", "")}"
        f"&end_date={end_date.replace("-", "")}"
    )

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        json_data = response.json()
        if isinstance(json_data, list) and len(json_data) > 0:
            df = pd.DataFrame(json_data)
            required_columns = {"bid", "timestamp"}
            if required_columns.issubset(df.columns):
                return (
                    df.assign(
                        timestamp=lambda df: pd.to_datetime(df["timestamp"], unit="s")
                    )
                    .rename(columns={"bid": "value", "timestamp": "date"})
                    .assign(date=lambda df: df["date"].dt.date)
                )[["date", "value"]]
            missing = required_columns - set(df.columns)
            logging.warning(f"Missing columns in API response: {missing}")
    except Exception as e:
        logging.error(f"Error fetching currency price: {e}")
        return None

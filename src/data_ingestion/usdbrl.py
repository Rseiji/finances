import requests
import psycopg2
from psycopg2 import OperationalError
from datetime import datetime
import sys
sys.path.append("/home/ubuntu/finances/")
from src.utils import persist_dataframe_to_database, read_sql_query
import pandas as pd
from dateutil.relativedelta import relativedelta


def get_currency_price(currency_symbol: str, start_date: int, end_date: int):
    num_days = (
        datetime.strptime(str(end_date), '%Y%m%d')
        - datetime.strptime(str(start_date), '%Y%m%d')
    ).days

    url = (
        f"https://economia.awesomeapi.com.br/json/daily/{currency_symbol}/{num_days}/"
        f"?start_date={start_date}&end_date={end_date}"
    )

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = pd.DataFrame(response.json()).assign(timestamp=lambda df: pd.to_datetime(df['timestamp'], unit='s'))
        return data
    except Exception as e:
        print(f"[{datetime.now()}] Erro ao buscar cotação: {e}")
        return None


def get_last_persisted_data(table_name: str = 'currencies.usdbrl'):
    max_min_ranges = read_sql_query(
        f"select max(date) as max_date from {table_name}"
    )
    return max_min_ranges['max_date'].iloc[0]

# TODO: solve weekends and holidays gaps. Either fill the data or make function to read
# this table already filling it.
# TODO: generalization for other currencies worth fetching
# TODO: reprocess the table with data only from this API
# TODO: apply code review
# TODO: solve sys.path
def run():
    prices = (
        get_currency_price(
            currency_symbol="USD-BRL",
            start_date=datetime.strftime(get_last_persisted_data() + relativedelta(days=1), "%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d")
        )
        .rename(columns={"bid": "value", "timestamp": "date"})
        .assign(date=lambda df: df["date"].dt.date)
    )[["date", "value"]]

    persist_dataframe_to_database(
        prices,
        schema="currencies",
        table="usdbrl",
        assign_processed_at_column=True,
    )


if __name__ == "__main__":
    run()

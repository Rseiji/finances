import re

import pandas as pd

from src.data_ingestion.google_sheets_automation import get_google_sheet_data
from src.finances_utils import process_new_trades
from src.utils import persist_dataframe_to_database, read_sql_query


def run_stocks() -> None:
    """Read and insert new stocks data into the database."""
    new_rows = get_google_sheet_data(worksheet_name="stocks", sheet_name="input_finantial_data")
    df_new = _format_stocks_data(pd.DataFrame(new_rows))
    current_avg_prices = _get_current_avg_prices()

    results = []
    for ticker, group in df_new.groupby("ticker"):
        current = current_avg_prices[current_avg_prices["ticker"] == ticker]
        avg_price_start = float(current["avg_price"].iloc[0]) if not current.empty else 0.0
        quantity_start = float(current["current_quantity"].iloc[0]) if not current.empty else 0.0

        processed = process_new_trades(group, avg_price_start, quantity_start)
        results.append(processed)

    df_result = pd.concat(results)
    persist_dataframe_to_database(
        df_result,
        "stocks",
        "transactions",
        True,
        pk_columns=["date", "ticker", "quantity", "price"]
    )


def _format_stocks_data(stocks: pd.DataFrame) -> pd.DataFrame:
    """Format stocks data to fit in the database."""
    return (
        stocks.assign(
            date=lambda df: pd.to_datetime(df["date"], format="%d/%m/%Y"),
            price=lambda df: df["price"].apply(_parse_brl_number).astype(float),
            taxes=lambda df: df["taxes"].apply(_parse_brl_number).astype(float)
        )
        .sort_values(["ticker", "date"], ascending=True)
    )


def _parse_brl_number(value: str) -> float:
    """Parse money columns with varied manual formats of BRL values."""
    s = str(value).strip()
    if s in ["R$  -", "R$-", "-", "R$ -"]:
        return 0.0
    cleaned = re.sub(r'R\$\s*', '', s)
    cleaned = re.sub(r'[^\d,.\-]', '', cleaned)
    if ',' in cleaned and '.' in cleaned:
        if cleaned.rfind('.') > cleaned.rfind(','):
            cleaned = cleaned.replace(',', '')
        else:
            cleaned = cleaned.replace('.', '').replace(',', '.')
    else:
        cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _get_current_avg_prices() -> pd.DataFrame:
    """Return current average prices of each stock ticker in the database."""
    return read_sql_query(
        """
        SELECT ticker, avg_price, current_quantity
        FROM (
            SELECT
                ticker,
                avg_price,
                current_quantity,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn
            FROM stocks.transactions
        ) sub
        WHERE rn = 1
        """
    )


def run_dividends() -> None:
    persist_dataframe_to_database(
        pd.DataFrame(
            get_google_sheet_data(
                worksheet_name="dividend_and_income",
                sheet_name="input_finantial_data"
            )
        ),
        "stocks",
        "dividends_incomes",
        True,
        pk_columns=["date", "ticker", "type", "source"],
    )


if __name__ == "__main__":
    run_stocks()
    # run_dividends()
    # run_allocations()
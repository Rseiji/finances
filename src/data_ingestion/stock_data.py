import pandas as pd
from src.data_ingestion.google_sheets_automation import get_google_sheet_data
from src.utils import read_sql_query, persist_dataframe_to_database
from src.finances_utils import process_new_trades


def run() -> None:
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
    format_prices_brl = lambda x: x.replace("R$", "").replace(",", ".")
    return (
        stocks.assign(
            date=lambda df: pd.to_datetime(df["date"], format="%d/%m/%Y"),
            price=lambda df: df["price"].apply(format_prices_brl).astype(float),
            taxes=lambda df: df["taxes"].apply(format_prices_brl).astype(float)
        )
        .sort_values(["ticker", "date"], ascending=True)
    )


# TODO: check the case where rebuying a stock after selling everything
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


if __name__ == "__main__":
    run()

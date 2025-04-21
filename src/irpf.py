"""Module to calculate brazilian yearly fiscal report for stock market transactions."""

from datetime import datetime, timedelta
from functools import partial

import pandas as pd

from utils import read_sql_query


def get_stocks_report(target_year: str) -> None:
    """
    Generates a comparative stocks report for the given target year and the previous year.

    The function retrieves stock reports for the target year and the previous year, merges them,
    and generates a comparison report. It also includes a textual message for each stock
    summarizing the report.

    Parameters
    ----------
    target_year : str
        The target year for which the stocks report is to be generated, in the format "YYYY".

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the comparative stocks report with the following columns:
        - "ticker": The stock ticker symbol.
        - "report_msg": A textual summary of the stock report.
        - "current_position_<previous_year>": The stock position in the previous year.
        - "current_position_<target_year>": The stock position in the target year.
        - "current_quantity_<previous_year>": The stock quantity in the previous year.
        - "current_quantity_<target_year>": The stock quantity in the target year.
        - "profit_<previous_year>": The profit for the stock in the previous year.
        - "profit_<target_year>": The profit for the stock in the target year.

    Notes
    -----
    - The function assumes the existence of `_get_year_stocks_report` to fetch yearly stock reports.
    - Missing data is filled with 0 during the merge operation.
    """
    previous_year = (
        datetime.strptime(target_year, "%Y") - timedelta(days=365)
    ).strftime("%Y")
    current_year_report = _get_year_stocks_report(target_year)
    last_year_report = _get_year_stocks_report(previous_year)

    reports_comparison = current_year_report.merge(
        last_year_report,
        how="outer",
        on="ticker",
        suffixes=(f"_{target_year}", f"_{previous_year}"),
    ).fillna(0)

    return (
        reports_comparison.assign(
            report_msg=lambda df: df.apply(
                partial(
                    _get_report_txt_msg,
                    target_year=target_year,
                ),
                axis=1,
            )
        ).query(
            f"~(current_position_{previous_year} == 0"
            f" & current_position_{target_year} == 0"
            f" & profit_{target_year} == 0)"
        )
    )[
        [
            "ticker",
            "report_msg",
            f"current_position_{previous_year}",
            f"current_position_{target_year}",
            f"current_quantity_{previous_year}",
            f"current_quantity_{target_year}",
            f"profit_{previous_year}",
            f"profit_{target_year}",
        ]
    ]


def _get_report_txt_msg(x, target_year: str):
    base_msg = f"{x['ticker']} - "
    match x[f"current_quantity_{target_year}"]:
        case quantity if quantity > 0:
            base_msg += f"{int(quantity)} ações emitidas. "
    match x[f"profit_{target_year}"]:
        case profit if profit < 0:
            base_msg += f"Prejuízo acumulado de -R${-profit:.2f}."
        case profit if profit > 0:
            base_msg += f"Lucro acumulado de R${profit:.2f}."
    return base_msg.rstrip(" - ")


def _get_year_stocks_report(target_year: str) -> pd.DataFrame:
    year_transactions = _fetch_year_transactions(target_year)
    current_positions = _calculate_current_positions(year_transactions)
    year_profits = _calculate_year_profits(year_transactions)
    year_report = _generate_year_report(current_positions, year_profits)
    return year_report


def _fetch_year_transactions(target_year: str) -> pd.DataFrame:
    """Fetch transactions for the target year from the database."""
    start_date = f"{target_year}-01-01"
    end_date = f"{target_year}-12-31"

    return read_sql_query(
        f"""
        SELECT *
        FROM stocks.transactions
        WHERE DATE BETWEEN date '{start_date}' AND date '{end_date}'
        ORDER BY ticker DESC, date DESC, current_quantity ASC
        """
    )


def _calculate_current_positions(year_transactions: pd.DataFrame) -> pd.DataFrame:
    """Calculate the current positions for each stock."""
    return (
        year_transactions.groupby("ticker", group_keys=False)
        .apply(_filter_last_date_rows)
        .assign(current_position=lambda df: df["avg_price"] * df["current_quantity"])
    )


# Assuming `year_transactions` is your dataframe
def _filter_last_date_rows(group):
    # Get the last date for the group
    last_date = group["date"].max()
    last_date_rows = group[group["date"] == last_date]

    if last_date_rows["quantity"].iloc[0] < 0:
        # For negative quantity, get the row with the smallest quantity
        return last_date_rows.loc[last_date_rows["current_quantity"].idxmin()]
    else:
        # For positive quantity, get the row with the greatest quantity
        return last_date_rows.loc[last_date_rows["current_quantity"].idxmax()]


def _calculate_year_profits(year_transactions: pd.DataFrame) -> pd.DataFrame:
    """Calculate the yearly profits for each stock."""
    return (
        year_transactions.query("quantity < 0")
        .assign(
            profit=lambda df: df["quantity"] * (-1) * (df["price"] - df["avg_price"])
            - df["taxes"]
        )
        .groupby("ticker", as_index=False)["profit"]
        .sum()
    )


def _generate_year_report(
    current_positions: pd.DataFrame, year_profits: pd.DataFrame
) -> pd.DataFrame:
    """Generate the final year report by merging positions and profits."""
    return (
        current_positions.reset_index(drop=True)[
            ["ticker", "current_quantity", "current_position"]
        ]
        .merge(year_profits, how="outer", on="ticker")
        .fillna(0)
    )

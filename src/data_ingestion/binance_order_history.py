"""
This module provides methods to parse Binance transaction files and convert them to a database-ready format.

Due to limitations in the Binance API for fetching complete historical trades, this module
processes transaction files downloaded from the Binance website. It parses swaps, converts, earnings, airdrops, deposits, and other operations, and prepares them for database insertion.

Main Functions:
- solve_parseable_swaps: Parses standard swap operations.
- solve_parseable_binance_convert: Handles Binance Convert operations.
- get_binance_earn: Extracts staking and earn rewards.
- get_airdrop_assets: Processes airdrop transactions.
- get_brl_deposits: Extracts BRL deposit records.
- parse_binance_report: Orchestrates parsing and returns all relevant tables.
- persist_transactions_database: Persists parsed data to the database.
- run: Main entry point for reading, parsing, and persisting Binance transaction data.

Usage:
    Run this module directly or call the `run()` function with a list of CSV file paths containing
    Binance transactions.
"""

from datetime import datetime
from functools import partial

import pandas as pd
from ..utils import persist_dataframe_to_database, read_sql_query

import logging


SWAP_CATEGS = [
    ["Transaction Spend", "Transaction Buy", "Transaction Fee"],
    ["Transaction Revenue", "Transaction Fee", "Transaction Sold"]
]
SWAP_TABLE_COLS = [
    "id",
    "date",
    "received_amount",
    "paid_taxes_amount",
    "paid_amount",
    "received_currency",
    "paid_taxes_currency",
    "paid_currency",
    "exchange_name",
]

format_date = partial(
    lambda x: int(datetime.strptime(x, "%Y-%m-%d").timestamp() * 1000)
)


def solve_parseable_swaps(df: pd.DataFrame) -> pd.DataFrame:
    """Solve swaps that have 2 or 3 rows in the Binance transactions dataframe.

    They are automatically parseable. Other swaps with more rows need manual input, and need
    to be treated separately and manually.
    """
    result = (
        df.merge(_get_dates_with_clean_swap(df))
        .groupby(["User_ID", "id", "Account", "Operation", "Coin"], as_index=False)
        .sum()
        .pivot(
            index=["User_ID", "id", "Account"],
            columns=["Operation"],
            values=["Change", "Coin"],
        )
        .reset_index()
        .assign(
            date=lambda df: pd.to_datetime(
                df["id"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
            exchange_name="binance",
        )
        .drop(columns=["User_ID", "Account"])
    )

    result.columns = [
        "_".join(col).strip("_") if isinstance(col, tuple) else col
        for col in result.columns.values
    ]

    result = result.rename(
        columns={
            "Change_Transaction Buy": "received_amount",
            "Change_Transaction Fee": "paid_taxes_amount",
            "Change_Transaction Spend": "paid_amount",
            "Coin_Transaction Buy": "received_currency",
            "Coin_Transaction Fee": "paid_taxes_currency",
            "Coin_Transaction Spend": "paid_currency",
        }
    )
    result["paid_amount"] *= -1
    result["paid_taxes_amount"] = result["paid_taxes_amount"].fillna(0)
    result["paid_taxes_amount"] *= -1

    return result[SWAP_TABLE_COLS]


def _get_dates_with_clean_swap(df: pd.DataFrame) -> pd.DataFrame:
    """Get dates with clean swaps (2 or 3 rows) from the Binance transactions dataframe."""
    swap_buy_count = (
        pd.concat([df.query("Operation.isin(@swap_categ)") for swap_categ in SWAP_CATEGS])
        .drop_duplicates()
        .groupby("id")
        .agg({"Operation": ["nunique", "size"]})
        .reset_index()
    )

    swap_buy_count = swap_buy_count[
        (
            swap_buy_count[("Operation", "nunique")]
            == swap_buy_count[("Operation", "size")]
        )
        & (swap_buy_count[("Operation", "nunique")].isin([2, 3]))
    ][["id"]]
    swap_buy_count.columns = ["id"]
    return swap_buy_count


def solve_parseable_binance_convert(df: pd.DataFrame) -> pd.DataFrame:
    """Get Binance Convert swap operations."""
    convert_ops = df[df["Operation"] == "Binance Convert"]
    utc_counts = convert_ops["id"].value_counts()
    valid_utcs = utc_counts[utc_counts == 2].index
    filtered = convert_ops[convert_ops["id"].isin(valid_utcs)]

    result = (
        filtered.groupby(["id"])
        .apply(
            lambda g: pd.Series(
                {
                    "paid_amount": (
                        -g.iloc[0]["Change"]
                        if g.iloc[0]["Change"] < 0
                        else -g.iloc[1]["Change"]
                    ),
                    "received_amount": (
                        g.iloc[0]["Change"]
                        if g.iloc[0]["Change"] > 0
                        else g.iloc[1]["Change"]
                    ),
                    "paid_currency": (
                        g.iloc[0]["Coin"]
                        if g.iloc[0]["Change"] < 0
                        else g.iloc[1]["Coin"]
                    ),
                    "received_currency": (
                        g.iloc[0]["Coin"]
                        if g.iloc[0]["Change"] > 0
                        else g.iloc[1]["Coin"]
                    ),
                }
            )
        )
        .reset_index()
        .assign(
            date=lambda df: pd.to_datetime(
                df["id"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
            exchange_name="binance",
            paid_taxes_amount=0,
            paid_taxes_currency=None,
        )
    )
    return result


def get_binance_earn(df: pd.DataFrame) -> pd.DataFrame:
    """Get Binance earn profits."""
    earn_categories = [
        "Staking Rewards",
        "Simple Earn Flexible Interest",
        "Simple Earn Locked Rewards",
    ]
    staking_earns = (
        df.query("Operation.isin(@earn_categories)")
        .rename(
            columns={
                "Coin": "currency",
                "Change": "earning_amount",
            }
        )
        .assign(
            source=lambda x: x["Operation"].apply(
                lambda op: (
                    "binance_staking"
                    if op == "Staking Rewards"
                    else "binance_simple_earn"
                )
            ),
            date=lambda df: pd.to_datetime(
                df["id"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
        )
    )[
        [
            "id",
            "date",
            "currency",
            "source",
            "earning_amount",
        ]
    ]
    return staking_earns


def get_airdrop_assets(df: pd.DataFrame) -> pd.DataFrame:
    """Get airdrop gain operations."""
    airdrop_assets = (
        df.query("Operation == 'Airdrop Assets'")
        .rename(
            columns={
                "Coin": "received_currency",
                "Change": "received_amount",
            }
        )
        .assign(
            date=lambda df: pd.to_datetime(
                df["id"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
            paid_taxes_amount=0,
            paid_amount=0,
            paid_taxes_currency=None,
            paid_currency=None,
            exchange_name="binance",
        )
    )

    return airdrop_assets[SWAP_TABLE_COLS]


def get_brl_deposits(df: pd.DataFrame) -> pd.DataFrame:
    """Get BRL deposits from the Binance transactions dataframe."""
    coin = "BRL"
    return (
        df.query(f"Operation == 'Deposit' & Coin == '{coin}'")
        .assign(
            date=lambda df: pd.to_datetime(df["id"], format="%Y-%m-%d %H:%M:%S").dt.date
        )
        .rename(columns={"Change": "value_brl"})
        .assign(exchange_name="binance")
    )[["id", "date", "value_brl", "exchange_name"]]


def get_manual_input_needed_swaps(
    df: pd.DataFrame, valid_swaps: pd.DataFrame
) -> pd.DataFrame:
    """Get timestamps that need manual input for swaps that have more than 3 rows."""
    df["date"] = pd.to_datetime(df["id"], format="%Y-%m-%d %H:%M:%S").dt.date
    result = (
        pd.concat([df.query("Operation.isin(@swap_categ)") for swap_categ in SWAP_CATEGS])
        .drop_duplicates()
        .merge(valid_swaps[["id"]].drop_duplicates(), how="left", indicator=True)
        .query("_merge == 'left_only'")
        .drop(columns=["_merge"])
    )
    return result


def get_manual_input_needed_converts(
    df: pd.DataFrame, solved_binance_convert: pd.DataFrame
) -> pd.DataFrame:
    """Return binance convert records that were not parsed and should be checked manually."""
    all_convert_utcs = set(df[df["Operation"] == "Binance Convert"]["id"])
    parsed_convert_utcs = set(solved_binance_convert["id"])
    not_parsed_binance_convert = all_convert_utcs - parsed_convert_utcs
    return df.query(
        "Operation == 'Binance Convert' and id.isin(@not_parsed_binance_convert)"
    )


def get_remaining_records(df: pd.DataFrame, **kwargs: pd.DataFrame) -> pd.DataFrame:
    """Return the remaining records."""
    remaining_utcs = set(df["id"])
    for _, table in kwargs.items():
        remaining_utcs -= set(table["id"])
    return df[df["id"].isin(remaining_utcs)].reset_index(drop=True)


def parse_binance_report(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Parse the Binance report and return a dictionary with the relevant data."""
    df = df.rename(columns={"UTC_Time": "id"})
    steps = [
        ("default_swaps", solve_parseable_swaps),
        ("converts", solve_parseable_binance_convert),
        ("earn", get_binance_earn),
        ("airdrops", get_airdrop_assets),
        ("brl_deposits", get_brl_deposits),
        (
            "earn_subscription",
            lambda df: df.query("Operation == 'Simple Earn Flexible Subscription'"),
        ),
        ("withdraws", lambda df: df.query("Operation == 'Withdraw'")),
    ]

    results = {}
    for key, func in steps:
        results[key] = func(df)

    results["manual_input_swaps"] = get_manual_input_needed_swaps(
        df, results["default_swaps"]
    )
    results["manual_input_needed_converts"] = get_manual_input_needed_converts(
        df, results["converts"]
    )

    results["remaining_records"] = get_remaining_records(df, **results)

    all_keys = set()
    for _, table in results.items():
        all_keys.update(set(table.id.unique()))
    if not (all_keys == set(df.id.unique())):
        raise ValueError(f"Missing keys in results: {set(df.id.unique()) - all_keys}")
    return results


def _preprocess_manual_inspection(df: pd.DataFrame) -> pd.DataFrame:
    categories_to_ignore = [
        "Simple Earn Flexible Redemption",
        "Simple Earn Flexible Airdrop",
        "Staking Purchase",
    ]

    ids = read_sql_query("SELECT DISTINCT id FROM crypto.manually_inserted_keys")

    df = (
        df.merge(ids, how='left', indicator=True)
        .query("_merge == 'left_only' and not (Operation.isin(@categories_to_ignore))")
        .drop(columns="_merge")
    )

    return df


def persist_transactions_database(
    results: dict[str, pd.DataFrame], manual_inspection_path: str
) -> None:
    """Persist the parsed Binance transactions to the database with upsert."""
    swaps = pd.concat(
        [results["default_swaps"], results["converts"], results["airdrops"]]
    )
    earn = results["earn"]
    brl_deposits = results["brl_deposits"]
    manual_inspection = pd.concat(
        [
            results["manual_input_swaps"],
            results["manual_input_needed_converts"],
            results["remaining_records"],
        ]
    ).pipe(_preprocess_manual_inspection)

    persist_dataframe_to_database(swaps, "crypto", "swaps", True, upsert=True, pk_columns=["id"])
    persist_dataframe_to_database(earn, "crypto", "earnings", True, upsert=True, pk_columns=["id"])
    persist_dataframe_to_database(brl_deposits, "crypto", "brl_deposits", True, upsert=True, pk_columns=["id"])

    logging.info(f"Manual inspection needed for {len(manual_inspection)} records.")
    logging.info(f"Persisting to manual inspection to {manual_inspection_path}")
    manual_inspection.to_csv(manual_inspection_path, index=False)


def read_binance_data(paths: list[str]) -> pd.DataFrame:
    """Read Binance transaction data from multiple CSV files and concatenate them into a single DataFrame."""
    data = []
    for path in paths:
        data.append(pd.read_csv(path))
    return pd.concat(data, ignore_index=True)


def run(paths: list[str] | None = None) -> None:
    """Run the Binance order history parsing and persistence."""
    paths = [
        "/home/ubuntu/finances/raw_data/binance/binance_transactions_2021.csv",
        "/home/ubuntu/finances/raw_data/binance/binance_transactions_2022.csv",
        "/home/ubuntu/finances/raw_data/binance/binance_transactions_2023.csv",
        "/home/ubuntu/finances/raw_data/binance/binance_transactions_2024.csv",
        "/home/ubuntu/finances/raw_data/binance/binance_transactions_202501_202506.csv",
    ]

    manual_inspection_path = "/home/ubuntu/finances/binance_manual_inspection.csv"

    df = read_binance_data(paths)
    results = parse_binance_report(df)
    persist_transactions_database(results, manual_inspection_path)


if __name__ == "__main__":
    run()

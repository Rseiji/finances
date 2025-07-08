"""Methods to parse binance transactions files and parse to DB format. Unfortunately,
the Binance API does not provide a way to fetch historical trades in its integrity (the data
comes incomplete), so I decided to download the trades from the Binance website
and parse them to the DB format. It's safer. It's possible to retry using the API
if needed.
"""

from datetime import datetime
from functools import partial

import pandas as pd


SWAP_CATEGS = ['Transaction Spend', 'Transaction Buy', 'Transaction Fee']

format_date = partial(lambda x: int(datetime.strptime(x, "%Y-%m-%d").timestamp() * 1000))


def get_brl_deposits(df, coin: str = "BRL") -> pd.DataFrame:
    return (
        df.query(f"Operation == 'Deposit' & Coin == '{coin}'")
        .assign(
            UTC_Time=lambda df: pd.to_datetime(df["UTC_Time"], format="%Y-%m-%d %H:%M:%S").dt.date
        )
        .rename(
            columns={
                "UTC_Time": "deposit_date",
                "Change": "value_brl",
            }
        )
        .assign(exchange_name="binance")
    )[["deposit_date", "value_brl", "exchange_name"]]


def get_binance_earn(df) -> pd.DataFrame:
    earn_categories = [
        "Staking Rewards",
        "Simple Earn Flexible Interest",
        "Simple Earn Locked Rewards",
    ]
    staking_earns = (
        l.query("Operation.isin(@earn_categories)")
        .rename(columns={
            "UTC_Time": "earning_date",
            "Coin": "earning_currency",
            "Change": "earning_amount",
        })
        .assign(
            source=lambda x: x["Operation"].apply(
                lambda op: "binance_staking" if op == "Staking Rewards" else "binance_simple_earn"
            ),
            earning_date=lambda df: pd.to_datetime(
                df["earning_date"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
        )
    )[[
        "earning_date",
        "earning_currency",
        "source",
        "earning_amount",
    ]]


def _get_dates_with_clean_swap(df) -> pd.DataFrame:
    swap_buy_count = (
        df.query("Operation.isin(@SWAP_CATEGS)")
        .groupby("UTC_Time")
        .agg({"Operation": ["nunique", "size"]})
        .reset_index()
    )

    swap_buy_count = swap_buy_count[
        (swap_buy_count[("Operation", "nunique")] == swap_buy_count[("Operation", "size")])
        & (swap_buy_count[("Operation", "nunique")].isin([2, 3]))
    ][["UTC_Time"]]
    swap_buy_count.columns = ["UTC_Time"]
    return swap_buy_count


def solve_swap_2_or_3_rows(df) -> pd.DataFrame:
    result = (
        df.merge(_get_dates_with_clean_swap(df))
        .groupby(
            ["User_ID", "UTC_Time", "Account", "Operation", "Coin"],
            as_index=False
        )
        .sum()
        .pivot(
            index=["User_ID", "UTC_Time", "Account"],
            columns=["Operation"],
            values=["Change", "Coin"]
        )
        .reset_index()
        .fillna(0.0)
        .assign(
            swap_date=lambda df: pd.to_datetime(
                df["UTC_Time"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
            exchange_name="binance",
        )
        .drop(columns=["User_ID", "Account"])
    )

    result.columns = [
        '_'.join(col).strip("_") if isinstance(col, tuple) else col
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
    result["paid_taxes_amount"] *= -1

    return result


def get_manual_input_needed_timestamps(df: pd.DataFrame, valid_swaps: pd.DataFrame) -> pd.DataFrame:
    df["swap_date"] = pd.to_datetime(
        df["UTC_Time"], format="%Y-%m-%d %H:%M:%S"
    ).dt.date
    result = df.query("Operation.isin(@SWAP_CATEGS)").merge(valid_swaps[["UTC_Time"]].drop_duplicates(), how="left", indicator=True).query(
        "_merge == 'left_only'"
    )
    return result


# TODO: case empty. Works?
def get_airdrop_assets(df):
    airdrop_assets = (
        df.query("Operation == 'Airdrop Assets'")
        .rename(
            columns={
                "Coin": "received_currency",
                "Change": "received_amount",
            }
        )
        .assign(
            swap_date=lambda df: pd.to_datetime(
                df["UTC_Time"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
            paid_taxes_amount=0,
            paid_amount=0,
            paid_taxes_currency=None,
            paid_currency=None,
            exchange_name="binance",
            _processed_at=datetime.now(),
        )
    )

    return airdrop_assets[[
        "swap_date",
        "UTC_Time",
        "received_amount",
        "paid_taxes_amount",
        "paid_amount",
        "received_currency",
        "paid_taxes_currency",
        "paid_currency",
        "exchange_name",
        "_processed_at",
    ]]


def get_valid_and_invalid_swap_dataframes(df: pd.DataFrame) -> pd.DataFrame:
    valid_swaps = pd.concat(
        [solve_swap_2_or_3_rows(df), get_airdrop_assets(df)],
        ignore_index=True
    )
    return valid_swaps

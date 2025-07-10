"""Methods to parse binance transactions files and parse to DB format. Unfortunately,
the Binance API does not provide a way to fetch historical trades in its integrity (the data
comes incomplete), so I decided to download the trades from the Binance website
and parse them to the DB format. It's safer. It's possible to retry using the API
if needed.
"""

from datetime import datetime
from functools import partial

import pandas as pd
from ..utils import persist_dataframe_to_database


SWAP_CATEGS = ['Transaction Spend', 'Transaction Buy', 'Transaction Fee']
SWAP_TABLE_COLS = [
    "id", "date", "received_amount", "paid_taxes_amount", "paid_amount",
    "received_currency", "paid_taxes_currency", "paid_currency", "exchange_name"
]

format_date = partial(lambda x: int(datetime.strptime(x, "%Y-%m-%d").timestamp() * 1000))


def get_brl_deposits(df, coin: str = "BRL") -> pd.DataFrame:
    return (
        df.query(f"Operation == 'Deposit' & Coin == '{coin}'")
        .assign(
            date=lambda df: pd.to_datetime(df["id"], format="%Y-%m-%d %H:%M:%S").dt.date
        )
        .rename(
            columns={
                "Change": "value_brl",
            }
        )
        .assign(exchange_name="binance")
    )[["id", "date", "value_brl", "exchange_name"]]


def get_binance_earn(df) -> pd.DataFrame:
    earn_categories = [
        "Staking Rewards",
        "Simple Earn Flexible Interest",
        "Simple Earn Locked Rewards",
    ]
    staking_earns = (
        df.query("Operation.isin(@earn_categories)")
        .rename(columns={
            "Coin": "currency",
            "Change": "earning_amount",
        })
        .assign(
            source=lambda x: x["Operation"].apply(
                lambda op: "binance_staking" if op == "Staking Rewards" else "binance_simple_earn"
            ),
            date=lambda df: pd.to_datetime(
                df["id"], format="%Y-%m-%d %H:%M:%S"
            ).dt.date,
        )
    )[[
        "id",
        "date",
        "currency",
        "source",
        "earning_amount",
    ]]
    return staking_earns


def _get_dates_with_clean_swap(df) -> pd.DataFrame:
    swap_buy_count = (
        df.query("Operation.isin(@SWAP_CATEGS)")
        .groupby("id")
        .agg({"Operation": ["nunique", "size"]})
        .reset_index()
    )

    swap_buy_count = swap_buy_count[
        (swap_buy_count[("Operation", "nunique")] == swap_buy_count[("Operation", "size")])
        & (swap_buy_count[("Operation", "nunique")].isin([2, 3]))
    ][["id"]]
    swap_buy_count.columns = ["id"]
    return swap_buy_count


def solve_swap_2_or_3_rows(df) -> pd.DataFrame:
    result = (
        df.merge(_get_dates_with_clean_swap(df))
        .groupby(
            ["User_ID", "id", "Account", "Operation", "Coin"],
            as_index=False
        )
        .sum()
        .pivot(
            index=["User_ID", "id", "Account"],
            columns=["Operation"],
            values=["Change", "Coin"]
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
    result["paid_taxes_amount"] = result["paid_taxes_amount"].fillna(0)
    result["paid_taxes_amount"] *= -1

    return result[SWAP_TABLE_COLS]


def get_manual_input_needed_timestamps(df: pd.DataFrame, valid_swaps: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(
        df["id"], format="%Y-%m-%d %H:%M:%S"
    ).dt.date
    result = df.query("Operation.isin(@SWAP_CATEGS)").merge(valid_swaps[["id"]].drop_duplicates(), how="left", indicator=True).query(
        "_merge == 'left_only'"
    ).drop(columns=["_merge"])
    return result


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


def solve_binance_convert(df) -> pd.DataFrame:
    convert_ops = df[df["Operation"] == "Binance Convert"]
    utc_counts = convert_ops["id"].value_counts()
    valid_utcs = utc_counts[utc_counts == 2].index
    filtered = convert_ops[convert_ops["id"].isin(valid_utcs)]

    result = (
        filtered
        .groupby(["id"])
        .apply(lambda g: pd.Series({
            "paid_amount": -g.iloc[0]["Change"] if g.iloc[0]["Change"] < 0 else -g.iloc[1]["Change"],
            "received_amount": g.iloc[0]["Change"] if g.iloc[0]["Change"] > 0 else g.iloc[1]["Change"],
            "paid_currency": g.iloc[0]["Coin"] if g.iloc[0]["Change"] < 0 else g.iloc[1]["Coin"],
            "received_currency": g.iloc[0]["Coin"] if g.iloc[0]["Change"] > 0 else g.iloc[1]["Coin"],
        }))
        .reset_index()
        .assign(
            date=lambda df: pd.to_datetime(df["id"], format="%Y-%m-%d %H:%M:%S").dt.date,
            exchange_name="binance",
            paid_taxes_amount=0,
            paid_taxes_currency=None,
        )
    )
    return result


def get_remaining_binance_convert_utcs(df, solved_binance_convert):
    """Return binance convert records that were not parsed and should be checked manually."""
    all_convert_utcs = set(df[df["Operation"] == "Binance Convert"]["id"])
    parsed_convert_utcs = set(solved_binance_convert["id"])
    not_parsed_binance_convert = all_convert_utcs - parsed_convert_utcs
    return df.query("Operation == 'Binance Convert' and id.isin(@not_parsed_binance_convert)")


def get_remaining_records(df, **kwargs):
    """Return the remaining records."""
    remaining_utcs = set(df["id"])
    for _, table in kwargs.items():
        remaining_utcs -= set(table["id"])
    return df[df["id"].isin(remaining_utcs)].reset_index(drop=True)


def parse_binance_report(df):
    """Parse the Binance report and return a dictionary with the relevant data."""
    df = df.rename(columns={"UTC_Time": "id"})
    steps = [
        ("default_swaps", solve_swap_2_or_3_rows),
        ("earn", get_binance_earn),
        ("airdrops", get_airdrop_assets),
        ("brl_deposits", get_brl_deposits),
        ("earn_subscription", lambda df: df.query("Operation == 'Simple Earn Flexible Subscription'")),
        ("converts", solve_binance_convert),
        ("withdraws", lambda df: df.query("Operation == 'Withdraw'")),
    ]

    results = {}
    for key, func in steps:
        results[key] = func(df)

    results["manual_input_swaps"] = get_manual_input_needed_timestamps(
        df, results["default_swaps"]
    )
    results["remaining_converts"] = get_remaining_binance_convert_utcs(
        df, results["converts"]
    )

    results["remaining_records"] = get_remaining_records(
        df, **results
    )

    all_keys = set()
    for key, table in results.items():
        all_keys.update(set(table.id.unique()))
    if not (all_keys == set(df.id.unique())):
        raise ValueError(f"Missing keys in results: {set(df.id.unique()) - all_keys}")
    return results


def persist_transactions_database(results):
    swaps = pd.concat(
        [results["default_swaps"], results["converts"], results["airdrops"]]
    )
    earn = results["earn"]
    brl_deposits = results["brl_deposits"]
    withdraws = results["withdraws"]
    manual_inspection = pd.concat([
        results["manual_input_swaps"],
        results["remaining_converts"],
        results["remaining_records"]
    ])

    persist_dataframe_to_database(swaps, "crypto", "swaps", True)
    persist_dataframe_to_database(earn, "crypto", "earnings", True)
    persist_dataframe_to_database(brl_deposits, "crypto", "brl_deposits", True)


def read_binance_data(paths: list):
    data = []
    for path in paths:
        data.append(pd.read_csv(path))
    return pd.concat(data, ignore_index=True)


def run(paths=[
    "/home/ubuntu/finances/raw_data/binance_transactions_2021.csv",
    "/home/ubuntu/finances/raw_data/binance_transactions_2022.csv",
    "/home/ubuntu/finances/raw_data/binance_transactions_2023.csv",
    "/home/ubuntu/finances/raw_data/binance_transactions_2024.csv",
    "/home/ubuntu/finances/raw_data/binance_transactions_2025_partial.csv"
]):
    df = read_binance_data(paths)
    results = parse_binance_report(df)
    persist_transactions_database(results)


if __name__ == "__main__":
    run()

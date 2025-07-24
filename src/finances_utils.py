"""Utilities to help in operational activities."""

import pandas as pd


def calculate_avg_price(df: pd.DataFrame) -> pd.DataFrame:
    """Given a pd.DataFrame with stocks transactional data, calculate the average price
    for each row.
    """
    df = df.sort_values(["ticker", "date"], ascending=True)

    for _, group in df.groupby("ticker"):
        avg_price = 0.0
        quantity = 0

        for idx, row in group.iterrows():
            q = row["quantity"]
            p = row["price"]
            t = row["taxes"]

            if q > 0:
                total_cost = avg_price * quantity + p * q + t
                quantity += q
                avg_price = total_cost / quantity if quantity > 0 else 0
            else:
                sale_qty = -q
                quantity -= sale_qty

            df.loc[idx, "avg_price"] = avg_price
            df.loc[idx, "current_quantity"] = quantity
    return df


def process_new_trades(
    df_new: pd.DataFrame,
    avg_price_start: float | int,
    quantity_start: float | int,
) -> pd.DataFrame:
    """
    Process new trades incrementally.

    Parameters
    ----------
    df_new : pd.DataFrame
        DataFrame containing new trade events, already sorted by date.
    avg_price_start : float
        Current average price before processing new trades.
    quantity_start : float
        Current quantity before processing new trades.

    Returns
    -------
    pd.DataFrame
        Updated DataFrame with `avg_price` and `current_quantity` columns.
    """
    df_new = df_new.copy()
    df_new["avg_price"] = 0.0
    df_new["current_quantity"] = 0

    avg_price: float = avg_price_start
    quantity: float = quantity_start

    for idx, row in df_new.iterrows():
        q: float = row["quantity"]
        p: float = row["price"]
        t: float = row["taxes"]

        if q > 0:
            total_cost: float = avg_price * quantity + p * q + t
            quantity += q
            avg_price = total_cost / quantity if quantity > 0 else 0
        else:
            sale_qty: float = -q
            quantity -= sale_qty

        df_new.loc[idx, "avg_price"] = avg_price
        df_new.loc[idx, "current_quantity"] = quantity

    return df_new

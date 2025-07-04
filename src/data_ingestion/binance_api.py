import requests
from datetime import datetime
from functools import partial
import os
import pandas as pd

BASE_URL = "https://api.binance.com/api/v3/"


def get_binance_close_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    **_: dict[str, str] | None
) -> list[tuple[str, float]]:
    """Fetch daily close prices for a given symbol between start_str and end_str."""
    format_date = partial(lambda x: int(datetime.strptime(x, "%Y-%m-%d").timestamp() * 1000))

    response = requests.get(
        os.path.join(BASE_URL, "klines"),
        params={
            "symbol": symbol.upper(),
            "interval": "1d",
            "startTime": format_date(start_date),
            "endTime": format_date(end_date),
            "limit": 1000
        }
    )

    return (
        pd.DataFrame(response.json())[[0, 4]]
        .rename(columns={0: "date", 4: "value"})
        .astype({"value": float})
        .assign(
            date=lambda df: pd.to_datetime(
                (df["date"] / 1000).astype(int), unit='s'
            ).dt.date
        )
    )

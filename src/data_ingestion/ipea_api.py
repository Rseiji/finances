import ipeadatapy
import pandas as pd


def _format_ipea_cdi(df: pd.DataFrame):
    df = df.rename(columns={"VALUE ((% a.m.))": "value", "DATE": "date"})
    return df


FORMATTING = {
    "BM12_TJCDI12": _format_ipea_cdi  # CDI
}


def get_ipea_close_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    **_: dict[str, str] | None
) -> pd.DataFrame | None:
    """Get data from ipea API."""
    try:
        df = (
            ipeadatapy.timeseries(symbol)
            .reset_index()
            .pipe(FORMATTING[symbol])
            .assign(date=lambda df: pd.to_datetime(df["date"]).dt.date)
            .sort_values("date")
        )
        return df[
            (df["date"] >= pd.to_datetime(start_date).date())
            & (df["date"] <= pd.to_datetime(end_date).date())
        ][["date", "value"]]
    except Exception as e:
        print(f"[IPEA] Erro ao buscar dados para {symbol}: {e}")
        return None




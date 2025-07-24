import yfinance as yf
import pandas as pd

def get_yfinance_close_prices(
    symbol: str,
    start_date: str,
    end_date: str,
    **_: dict[str, str] | None
) -> pd.DataFrame | None:
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(start=start_date, end=end_date)

        if hist.empty:
            return None

        df = hist[["Close"]].reset_index()
        df.rename(columns={"Close": "value", "Date": "date"}, inplace=True)
        df["date"] = df["date"].dt.date  # garantir datetime.date
        return df[["date", "value"]]
    except Exception as e:
        print(f"[yfinance] Erro ao buscar dados para {symbol}: {e}")
        return None

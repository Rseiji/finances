import pandas as pd
import pytest
from src.finances_utils import calculate_avg_price, process_new_trades


def test_process_new_trades_buy_only():
    df = pd.DataFrame([
        {"quantity": 10, "price": 5.0, "taxes": 1.0},
        {"quantity": 5, "price": 6.0, "taxes": 0.5},
    ])
    result = process_new_trades(df, avg_price_start=0.0, quantity_start=0)
    # First buy: avg_price = (0*0 + 5*10 + 1) / 10 = 5.1
    # Second buy: avg_price = (5.1*10 + 6*5 + 0.5) / 15 = (51 + 30 + 0.5)/15 = 81.5/15 = 5.433...
    expected_first_avg_price = (0*0 + 5*10 + 1) / 10
    expected_second_avg_price = (expected_first_avg_price*10 + 6*5 + 0.5) / 15
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == 81.5 / 15  # 5.433...
    assert result.loc[0, "current_quantity"] == 10
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == expected_second_avg_price
    assert result.loc[1, "current_quantity"] == 15


def test_process_new_trades_buy_and_sell():
    df = pd.DataFrame([
        {"quantity": 10, "price": 5.0, "taxes": 1.0},
        {"quantity": -4, "price": 0.0, "taxes": 0.0},
        {"quantity": 6, "price": 7.0, "taxes": 0.5},
    ])
    result = process_new_trades(df, avg_price_start=0.0, quantity_start=0)
    # After first buy: avg_price = 5.1, quantity = 10
    # After sell: quantity = 6, avg_price unchanged
    # After second buy: avg_price = (5.1*6 + 7*6 + 0.5)/12 = (30.6 + 42 + 0.5)/12 = 73.1/12 = 6.0916...
    assert pytest.approx(result.loc[0, "avg_price"], 0.001) == 5.1
    assert result.loc[0, "current_quantity"] == 10
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == 5.1
    assert result.loc[1, "current_quantity"] == 6
    assert pytest.approx(result.loc[2, "avg_price"], 0.001) == 73.1 / 12
    assert result.loc[2, "current_quantity"] == 12


def test_process_new_trades_starting_values():
    df = pd.DataFrame([
        {"quantity": 5, "price": 8.0, "taxes": 2.0},
        {"quantity": -2, "price": 0.0, "taxes": 0.0},
    ])
    # Start with avg_price=10, quantity=3
    result = process_new_trades(df, avg_price_start=10.0, quantity_start=3)
    # First buy: total_cost = 10*3 + 8*5 + 2 = 30 + 40 + 2 = 72, quantity = 8, avg_price = 72/8 = 9.0
    # Sell: quantity = 6, avg_price unchanged
    assert pytest.approx(result.loc[0, "avg_price"], 0.001) == 9.0
    assert result.loc[0, "current_quantity"] == 8
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == 9.0
    assert result.loc[1, "current_quantity"] == 6


def test_process_new_trades_zero_quantity():
    df = pd.DataFrame([
        {"quantity": 0, "price": 10.0, "taxes": 0.0},
    ])
    result = process_new_trades(df, avg_price_start=5.0, quantity_start=10)
    # No change expected
    assert result.loc[0, "avg_price"] == 5.0
    assert result.loc[0, "current_quantity"] == 10


def test_process_new_trades_empty_df():
    df = pd.DataFrame(columns=["quantity", "price", "taxes"])
    result = process_new_trades(df, avg_price_start=0.0, quantity_start=0)
    assert result.empty


def test_calculate_avg_price_single_ticker_buy_only():
    df = pd.DataFrame([
        {"ticker": "AAPL", "date": "2024-01-01", "quantity": 10, "price": 5.0, "taxes": 1.0},
        {"ticker": "AAPL", "date": "2024-01-02", "quantity": 5, "price": 6.0, "taxes": 0.5},
    ])
    result = calculate_avg_price(df)
    assert pytest.approx(result.loc[0, "avg_price"], 0.001) == 5.1
    assert result.loc[0, "current_quantity"] == 10
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == 5.433
    assert result.loc[1, "current_quantity"] == 15


def test_calculate_avg_price_single_ticker_buy_and_sell():
    df = pd.DataFrame([
        {"ticker": "AAPL", "date": "2024-01-01", "quantity": 10, "price": 5.0, "taxes": 1.0},
        {"ticker": "AAPL", "date": "2024-01-02", "quantity": -4, "price": 0.0, "taxes": 0.0},
        {"ticker": "AAPL", "date": "2024-01-03", "quantity": 6, "price": 7.0, "taxes": 0.5},
    ])
    result = calculate_avg_price(df)
    assert pytest.approx(result.loc[0, "avg_price"], 0.001) == 5.1
    assert result.loc[0, "current_quantity"] == 10
    assert pytest.approx(result.loc[1, "avg_price"], 0.001) == 5.1
    assert result.loc[1, "current_quantity"] == 6
    assert pytest.approx(result.loc[2, "avg_price"], 0.001) == 6.092
    assert result.loc[2, "current_quantity"] == 12


def test_calculate_avg_price_multiple_tickers():
    df = pd.DataFrame([
        {"ticker": "AAPL", "date": "2024-01-01", "quantity": 10, "price": 5.0, "taxes": 1.0},
        {"ticker": "GOOG", "date": "2024-01-01", "quantity": 8, "price": 10.0, "taxes": 2.0},
        {"ticker": "AAPL", "date": "2024-01-02", "quantity": 5, "price": 6.0, "taxes": 0.5},
        {"ticker": "GOOG", "date": "2024-01-02", "quantity": -3, "price": 0.0, "taxes": 0.0},
    ])
    result = calculate_avg_price(df)
    # AAPL
    assert pytest.approx(result[result["ticker"] == "AAPL"].iloc[0]["avg_price"], 0.001) == 5.1
    assert result[result["ticker"] == "AAPL"].iloc[0]["current_quantity"] == 10
    assert pytest.approx(result[result["ticker"] == "AAPL"].iloc[1]["avg_price"], 0.001) == 5.433
    assert result[result["ticker"] == "AAPL"].iloc[1]["current_quantity"] == 15
    # GOOG
    assert pytest.approx(result[result["ticker"] == "GOOG"].iloc[0]["avg_price"], 0.001) == 10.25
    assert result[result["ticker"] == "GOOG"].iloc[0]["current_quantity"] == 8
    assert pytest.approx(result[result["ticker"] == "GOOG"].iloc[1]["avg_price"], 0.001) == 10.25
    assert result[result["ticker"] == "GOOG"].iloc[1]["current_quantity"] == 5


def test_calculate_avg_price_zero_quantity():
    df = pd.DataFrame([
        {"ticker": "AAPL", "date": "2024-01-01", "quantity": 0, "price": 10.0, "taxes": 0.0},
    ])
    result = calculate_avg_price(df)
    assert result.loc[0, "avg_price"] == 0.0
    assert result.loc[0, "current_quantity"] == 0


def test_calculate_avg_price_empty_df():
    df = pd.DataFrame(columns=["ticker", "date", "quantity", "price", "taxes"])
    result = calculate_avg_price(df)
    assert result.empty

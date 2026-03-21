import math
import httpx
from typing import List
from sqlalchemy.orm import Session
from db.models import PriceSnapshot, RiskMetric
from datetime import datetime, timedelta

# Alpha Vantage key for fetching SPY benchmark data
AV_BASE = "https://www.alphavantage.co/query"
AV_KEY  = "NTMC4940Y5O7O5AV"

# Cache SPY returns so we don't fetch on every risk compute
_spy_returns_cache = {"returns": None, "fetched_at": None}
SPY_CACHE_TTL_HOURS = 24  # refresh once a day


def get_spy_returns() -> List[float]:
    """
    Fetch S&P 500 (SPY ETF) daily returns from Alpha Vantage.
    Used as market benchmark for Alpha and Beta calculations.
    Cached for 24 hours to avoid hitting API rate limits.
    """
    global _spy_returns_cache

    now = datetime.utcnow()
    if (
        _spy_returns_cache["returns"] is not None
        and _spy_returns_cache["fetched_at"] is not None
        and (now - _spy_returns_cache["fetched_at"]).total_seconds() < SPY_CACHE_TTL_HOURS * 3600
    ):
        return _spy_returns_cache["returns"]

    try:
        resp = httpx.get(
            AV_BASE,
            params={
                "function":   "TIME_SERIES_DAILY",
                "symbol":     "SPY",
                "outputsize": "compact",
                "apikey":     AV_KEY,
            },
            timeout=httpx.Timeout(30.0, connect=10.0)
        )
        data        = resp.json()
        time_series = data.get("Time Series (Daily)")

        prices = [
            float(v["4. close"])
            for _, v in sorted(time_series.items())
        ]
        returns = calculate_returns(prices)

        _spy_returns_cache["returns"]   = returns
        _spy_returns_cache["fetched_at"] = now
       
    except Exception as e:
        print(f"[SPY] Failed to fetch benchmark data: {e}")
        return []


def calculate_returns(prices: List[float]) -> List[float]:
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))
    return returns


def calculate_volatility(returns: List[float]) -> float:
    """
    Annualised volatility from daily log returns.
    Filters out extreme returns that come from synthetic/bad data.
    """
    if len(returns) < 2:
        return 0.0
    filtered = [r for r in returns if abs(r) < 0.15]
    if len(filtered) < 2:
        filtered = returns
    n    = len(filtered)
    mean = sum(filtered) / n
    var  = sum((r - mean) ** 2 for r in filtered) / (n - 1)
    daily = math.sqrt(var)
    ann   = round(daily * math.sqrt(252), 6)
    return min(ann, 1.50)


def calculate_var_95(returns: List[float], price: float) -> float:
    if len(returns) < 10:
        return 0.0
    sorted_returns = sorted(returns)
    index          = max(int(len(sorted_returns) * 0.05), 1)
    return round(abs(sorted_returns[index] * price), 4)


def calculate_beta(stock_returns: List[float], market_returns: List[float]) -> float:
    n = min(len(stock_returns), len(market_returns))
    if n < 5:
        return 1.0
    s = stock_returns[:n]
    m = market_returns[:n]
    mean_s = sum(s) / n
    mean_m = sum(m) / n
    cov = sum((s[i] - mean_s) * (m[i] - mean_m) for i in range(n)) / (n - 1)
    var = sum((m[i] - mean_m) ** 2 for i in range(n)) / (n - 1)
    return round(cov / var, 4) if var != 0 else 1.0


def calculate_alpha(
    stock_returns: List[float],
    market_returns: List[float],
    beta: float,
    risk_free_rate: float = 0.05
) -> float:
    """
    Jensen's Alpha — measures how much a stock outperforms or underperforms
    what would be expected given its Beta and the market return.

    Formula: Alpha = Stock Return - [Risk Free Rate + Beta × (Market Return - Risk Free Rate)]

    Positive alpha = stock beat the market on a risk-adjusted basis
    Negative alpha = stock underperformed vs what its beta predicts
    Zero alpha     = performed exactly as expected given market exposure

    Returns annualised alpha as a percentage (e.g. 2.5 = +2.5% above expected).
    """
    n = min(len(stock_returns), len(market_returns))
    if n < 5 or beta is None:
        return 0.0

    s = stock_returns[:n]
    m = market_returns[:n]

    # Annualise mean daily returns
    mean_stock  = (sum(s) / n) * 252
    mean_market = (sum(m) / n) * 252

    # Jensen's Alpha
    expected_return = risk_free_rate + beta * (mean_market - risk_free_rate)
    alpha = mean_stock - expected_return

    return round(alpha * 100, 4)  # return as percentage


def calculate_sharpe(returns: List[float], risk_free_rate: float = 0.05) -> float:
    """
    Annualised Sharpe ratio.
    """
    if len(returns) < 2:
        return 0.0
    n           = len(returns)
    mean_daily  = sum(returns) / n
    mean_annual = mean_daily * 252
    std_daily   = math.sqrt(sum((r - mean_daily) ** 2 for r in returns) / (n - 1))
    annual_vol  = std_daily * math.sqrt(252)
    if annual_vol == 0:
        return 0.0
    return round((mean_annual - risk_free_rate) / annual_vol, 4)


def calculate_expected_move(price: float, volatility: float, days_to_earnings: int = 1) -> dict:
    if volatility == 0 or price == 0:
        return {"dollar": 0.0, "pct": 0.0}
    move_pct    = volatility * math.sqrt(days_to_earnings / 252)
    move_dollar = price * move_pct
    return {
        "dollar": round(move_dollar, 2),
        "pct":    round(move_pct * 100, 2)
    }


def calculate_position_size(
    portfolio_value: float,
    price: float,
    var_95: float,
    risk_pct: float = 0.01
) -> dict:
    if var_95 == 0 or price == 0:
        return {"shares": 0, "position_value": 0.0, "risk_dollar": 0.0}

    max_risk_dollar = portfolio_value * risk_pct
    shares          = int(max_risk_dollar / var_95)
    position_value  = round(shares * price, 2)
    risk_dollar     = round(shares * var_95, 2)

    return {
        "shares":          shares,
        "position_value":  position_value,
        "risk_dollar":     risk_dollar,
        "risk_pct_actual": round((risk_dollar / portfolio_value) * 100, 3)
    }


def compute_and_store_risk(db: Session, symbol: str):
    rows = db.query(PriceSnapshot)\
             .filter_by(symbol=symbol)\
             .order_by(PriceSnapshot.timestamp.asc())\
             .all()

    prices = [r.price for r in rows]
    if len(prices) < 3:
        print(f"[Risk] {symbol}: not enough data ({len(prices)} snapshots), skipping")
        return

    returns = calculate_returns(prices)
    vol     = calculate_volatility(returns)
    var_95  = calculate_var_95(returns, prices[-1])
    sharpe  = calculate_sharpe(returns)

    # Get SPY benchmark returns (cached, fetched from Alpha Vantage)
    spy_returns = get_spy_returns()

    # Also check DB for SPY as fallback
    if not spy_returns:
        spy_rows   = db.query(PriceSnapshot).filter_by(symbol="SPY")\
                      .order_by(PriceSnapshot.timestamp.asc()).all()
        spy_prices = [r.price for r in spy_rows]
        spy_returns = calculate_returns(spy_prices) if len(spy_prices) >= 3 else []

    beta  = calculate_beta(returns, spy_returns)  if len(spy_returns) >= 5 else 1.0
    alpha = calculate_alpha(returns, spy_returns, beta) if len(spy_returns) >= 5 else 0.0

    record = RiskMetric(
        symbol=symbol,
        var_95=var_95,
        volatility_30d=vol,
        beta=beta,
        alpha=alpha,
        computed_at=datetime.utcnow()
    )
    db.add(record)
    db.commit()
    return {"vol": vol, "var_95": var_95, "beta": beta, "alpha": alpha, "sharpe": sharpe, "price": prices[-1]}
import math
import httpx
from collections import OrderedDict
from typing import List, Dict
from sqlalchemy.orm import Session
from db.models import PriceSnapshot, RiskMetric
from datetime import datetime, timedelta

# Alpha Vantage for SPY benchmark
AV_BASE = "https://www.alphavantage.co/query"
AV_KEY  = "NTMC4940Y5O7O5AV"

# Cache SPY returns — refresh once per day
_spy_returns_cache = {"returns": None, "fetched_at": None}
SPY_CACHE_TTL_HOURS = 24


def get_spy_returns() -> List[float]:
    """
    Fetch SPY daily returns from Alpha Vantage.
    Cached 24hrs to stay within 25 calls/day free tier limit.
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

        if not time_series:
            return []

        prices  = [float(v["4. close"]) for _, v in sorted(time_series.items())]
        returns = calculate_returns(prices)

        _spy_returns_cache["returns"]    = returns
        _spy_returns_cache["fetched_at"] = now
        return returns

    except Exception:
        return []


def calculate_returns(prices: List[float]) -> List[float]:
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))
    return returns


def calculate_volatility(returns: List[float]) -> float:
    """Annualised volatility from daily log returns."""
    if len(returns) < 2:
        return 0.0
    filtered = [r for r in returns if abs(r) < 0.15]
    if len(filtered) < 2:
        filtered = returns
    n     = len(filtered)
    mean  = sum(filtered) / n
    var   = sum((r - mean) ** 2 for r in filtered) / (n - 1)
    daily = math.sqrt(var)
    ann   = round(daily * math.sqrt(252), 6)
    return min(ann, 1.50)


def calculate_var_95(returns: List[float], price: float) -> float:
    if len(returns) < 10:
        return 0.0
    sorted_r = sorted(returns)
    index    = max(int(len(sorted_r) * 0.05), 1)
    return round(abs(sorted_r[index] * price), 4)


def calculate_var_99(returns: List[float], price: float) -> float:
    """VaR at 99% confidence — more conservative than 95%."""
    if len(returns) < 10:
        return 0.0
    sorted_r = sorted(returns)
    index    = max(int(len(sorted_r) * 0.01), 1)
    return round(abs(sorted_r[index] * price), 4)


def calculate_max_drawdown(prices: List[float]) -> float:
    """
    Maximum drawdown — largest peak-to-trough decline in %.
    Tells you the worst loss from a peak you would have suffered.
    e.g. -25.3 means the stock fell 25.3% from its peak at some point.
    """
    if len(prices) < 2:
        return 0.0
    peak         = prices[0]
    max_drawdown = 0.0
    for price in prices:
        if price > peak:
            peak = price
        drawdown = (price - peak) / peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return round(max_drawdown * 100, 4)  # as percentage


def calculate_sortino(returns: List[float], risk_free_rate: float = 0.05) -> float:
    """
    Sortino ratio — like Sharpe but only penalizes DOWNSIDE volatility.
    Better measure for risk-adjusted return since investors only care
    about downside risk, not upside volatility.
    Sortino > 1 = good, > 2 = very good.
    """
    if len(returns) < 2:
        return 0.0
    n           = len(returns)
    mean_daily  = sum(returns) / n
    mean_annual = mean_daily * 252

    # Only negative returns contribute to downside deviation
    downside = [r for r in returns if r < 0]
    if len(downside) < 2:
        return 0.0

    downside_std   = math.sqrt(sum(r ** 2 for r in downside) / len(downside))
    downside_annual = downside_std * math.sqrt(252)

    if downside_annual == 0:
        return 0.0
    return round((mean_annual - risk_free_rate) / downside_annual, 4)


def calculate_beta(stock_returns: List[float], market_returns: List[float]) -> float:
    n = min(len(stock_returns), len(market_returns))
    if n < 5:
        return 1.0
    s      = stock_returns[:n]
    m      = market_returns[:n]
    mean_s = sum(s) / n
    mean_m = sum(m) / n
    cov    = sum((s[i] - mean_s) * (m[i] - mean_m) for i in range(n)) / (n - 1)
    var    = sum((m[i] - mean_m) ** 2 for i in range(n)) / (n - 1)
    return round(cov / var, 4) if var != 0 else 1.0


def calculate_alpha(
    stock_returns: List[float],
    market_returns: List[float],
    beta: float,
    risk_free_rate: float = 0.05
) -> float:
    """
    Jensen's Alpha — outperformance vs what Beta predicts.
    Positive = beat market on risk-adjusted basis.
    Returns annualised alpha as percentage.
    """
    n = min(len(stock_returns), len(market_returns))
    if n < 5 or beta is None:
        return 0.0
    s = stock_returns[:n]
    m = market_returns[:n]
    mean_stock  = (sum(s) / n) * 252
    mean_market = (sum(m) / n) * 252
    expected    = risk_free_rate + beta * (mean_market - risk_free_rate)
    return round((mean_stock - expected) * 100, 4)


def calculate_sharpe(returns: List[float], risk_free_rate: float = 0.05) -> float:
    """Annualised Sharpe ratio."""
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


def calculate_correlation_matrix(symbols_returns: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    """
    Correlation matrix between all symbols.
    Shows how stocks move together — useful for diversification.
    +1 = perfect correlation, 0 = no correlation, -1 = inverse.
    """
    matrix = {}
    symbols = list(symbols_returns.keys())

    for s1 in symbols:
        matrix[s1] = {}
        for s2 in symbols:
            r1 = symbols_returns[s1]
            r2 = symbols_returns[s2]
            n  = min(len(r1), len(r2))
            if n < 5:
                matrix[s1][s2] = 1.0 if s1 == s2 else 0.0
                continue
            a = r1[:n]
            b = r2[:n]
            mean_a = sum(a) / n
            mean_b = sum(b) / n
            cov    = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n)) / (n - 1)
            std_a  = math.sqrt(sum((x - mean_a) ** 2 for x in a) / (n - 1))
            std_b  = math.sqrt(sum((x - mean_b) ** 2 for x in b) / (n - 1))
            if std_a == 0 or std_b == 0:
                matrix[s1][s2] = 1.0 if s1 == s2 else 0.0
            else:
                matrix[s1][s2] = round(cov / (std_a * std_b), 4)

    return matrix


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

    # Deduplicate to one price per day (last price of each day)
    # This ensures returns are daily returns, not intraday noise
    daily = OrderedDict()
    for r in rows:
        date_key = r.timestamp.strftime("%Y-%m-%d")
        daily[date_key] = r.price
    prices = list(daily.values())

    if len(prices) < 3:
        return

    returns = calculate_returns(prices)
    vol     = calculate_volatility(returns)
    var_95  = calculate_var_95(returns, prices[-1])
    var_99  = calculate_var_99(returns, prices[-1])
    sharpe  = calculate_sharpe(returns)
    sortino = calculate_sortino(returns)
    max_dd  = calculate_max_drawdown(prices)

    # SPY benchmark for alpha/beta
    spy_returns = get_spy_returns()
    if not spy_returns:
        spy_rows    = db.query(PriceSnapshot).filter_by(symbol="SPY")\
                       .order_by(PriceSnapshot.timestamp.asc()).all()
        spy_daily   = OrderedDict()
        for r in spy_rows:
            spy_daily[r.timestamp.strftime("%Y-%m-%d")] = r.price
        spy_prices  = list(spy_daily.values())
        spy_returns = calculate_returns(spy_prices) if len(spy_prices) >= 3 else []

    beta  = calculate_beta(returns, spy_returns)  if len(spy_returns) >= 5 else 1.0
    alpha = calculate_alpha(returns, spy_returns, beta) if len(spy_returns) >= 5 else 0.0

    record = RiskMetric(
        symbol=symbol,
        var_95=var_95,
        var_99=var_99,
        volatility_30d=vol,
        beta=beta,
        alpha=alpha,
        max_drawdown=max_dd,
        sortino=sortino,
        computed_at=datetime.utcnow()
    )
    db.add(record)
    db.commit()
    return {
        "vol": vol, "var_95": var_95, "var_99": var_99,
        "beta": beta, "alpha": alpha, "sharpe": sharpe,
        "sortino": sortino, "max_drawdown": max_dd,
        "price": prices[-1]
    }
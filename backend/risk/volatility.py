import math
from typing import List
from sqlalchemy.orm import Session
from db.models import PriceSnapshot, RiskMetric
from datetime import datetime


def calculate_returns(prices: List[float]) -> List[float]:
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))
    return returns


def calculate_volatility(returns: List[float]) -> float:
    if len(returns) < 2:
        return 0.0
    n     = len(returns)
    mean  = sum(returns) / n
    var   = sum((r - mean) ** 2 for r in returns) / (n - 1)
    daily = math.sqrt(var)
    return round(daily * math.sqrt(252), 6)


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


def calculate_sharpe(returns: List[float], risk_free_rate: float = 0.05) -> float:
    """
    Annualised Sharpe ratio.
    risk_free_rate = annual rate (default 5% = current approx US risk-free).
    Sharpe = (mean_annual_return - risk_free) / annual_vol
    """
    if len(returns) < 2:
        return 0.0
    n            = len(returns)
    mean_daily   = sum(returns) / n
    mean_annual  = mean_daily * 252
    std_daily    = math.sqrt(sum((r - mean_daily) ** 2 for r in returns) / (n - 1))
    annual_vol   = std_daily * math.sqrt(252)
    if annual_vol == 0:
        return 0.0
    return round((mean_annual - risk_free_rate) / annual_vol, 4)


def calculate_expected_move(price: float, volatility: float, days_to_earnings: int = 1) -> dict:
    """
    Expected move around earnings using historical vol as IV proxy.
    Formula: price × vol × sqrt(days / 252)
    Returns both dollar move and percentage move.
    """
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
    """
    Fixed fractional position sizing based on VaR.
    risk_pct = max % of portfolio to risk per trade (default 1%).
    max_risk_dollar = portfolio_value × risk_pct
    shares = max_risk_dollar / var_95 (VaR = max expected daily loss per share)
    """
    if var_95 == 0 or price == 0:
        return {"shares": 0, "position_value": 0.0, "risk_dollar": 0.0}

    max_risk_dollar = portfolio_value * risk_pct
    shares          = int(max_risk_dollar / var_95)
    position_value  = round(shares * price, 2)
    risk_dollar     = round(shares * var_95, 2)

    return {
        "shares":         shares,
        "position_value": position_value,
        "risk_dollar":    risk_dollar,
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

    returns  = calculate_returns(prices)
    vol      = calculate_volatility(returns)
    var_95   = calculate_var_95(returns, prices[-1])
    sharpe   = calculate_sharpe(returns)

    spy_rows    = db.query(PriceSnapshot).filter_by(symbol="SPY")\
                   .order_by(PriceSnapshot.timestamp.asc()).all()
    spy_prices  = [r.price for r in spy_rows]
    beta        = calculate_beta(returns, calculate_returns(spy_prices)) \
                  if len(spy_prices) >= 3 else 1.0

    record = RiskMetric(
        symbol=symbol,
        var_95=var_95,
        volatility_30d=vol,
        beta=beta,
        computed_at=datetime.utcnow()
    )
    db.add(record)
    db.commit()
    print(f"[Risk] {symbol}: vol={vol:.4f} var95=${var_95:.2f} beta={beta} sharpe={sharpe:.2f}")
    return {"vol": vol, "var_95": var_95, "beta": beta, "sharpe": sharpe, "price": prices[-1]}
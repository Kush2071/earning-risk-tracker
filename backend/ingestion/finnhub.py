import httpx
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# Finnhub — quotes, news, search, earnings
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_KEY  = "d6ti17pr01qhkb449fq0d6ti17pr01qhkb449fqg"
FINNHUB_HEADERS = {"X-Finnhub-Token": FINNHUB_KEY}

# Alpha Vantage — historical daily + intraday candles (free tier)
AV_BASE = "https://www.alphavantage.co/query"
AV_KEY  = "NTMC4940Y5O7O5AV"

TIMEOUT = httpx.Timeout(30.0, connect=10.0)

# Use ET everywhere
ET = ZoneInfo("America/New_York")


def now_et() -> datetime:
    """Current time in ET."""
    return datetime.now(ET)


def today_et() -> datetime:
    """Today's date in ET at midnight."""
    n = now_et()
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


def last_trading_day_et() -> datetime:
    """
    Returns the most recent weekday in ET.
    Saturday → Friday, Sunday → Friday, weekday → today.
    """
    n = now_et()
    weekday = n.weekday()  # 0=Mon, 5=Sat, 6=Sun
    if weekday == 5:       # Saturday
        return (n - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif weekday == 6:     # Sunday
        return (n - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


BULLISH_WORDS = [
    "beat", "surpass", "record", "growth", "upgrade", "strong", "profit",
    "rise", "gain", "positive", "exceed", "outperform", "rally", "surge",
    "soar", "jump", "climb", "boost", "expand", "innovation", "launch",
    "partner", "deal", "win", "bullish", "opportunity", "demand", "revenue",
    "autonomous", "ai", "electric", "ev", "adoption", "milestone", "delivery",
    "breakthrough", "invest", "buy", "upside", "momentum", "recovery"
]
BEARISH_WORDS = [
    "miss", "below", "weak", "downgrade", "loss", "decline", "fall", "cut",
    "disappoint", "risk", "concern", "lawsuit", "recall", "investigation",
    "probe", "fine", "penalty", "short", "bearish", "sell", "crash", "drop",
    "plunge", "slump", "layoff", "delay", "competition", "rival", "pressure",
    "warning", "deficit", "debt", "volatile", "uncertain", "headwind", "slow"
]


def score_headline(headline: str) -> float:
    text  = headline.lower()
    bulls = sum(1 for w in BULLISH_WORDS if w in text)
    bears = sum(1 for w in BEARISH_WORDS if w in text)
    total = bulls + bears
    if total == 0:
        return 0.0
    return round((bulls - bears) / total, 4)


# ─── FINNHUB FUNCTIONS (quotes, news, search, earnings) ──────────────────────

def get_earnings_calendar(from_date: str = None, to_date: str = None) -> list:
    if not from_date:
        from_date = today_et().strftime("%Y-%m-%d")
    if not to_date:
        to_date = (today_et() + timedelta(days=14)).strftime("%Y-%m-%d")
    resp = httpx.get(
        f"{FINNHUB_BASE}/calendar/earnings",
        headers=FINNHUB_HEADERS,
        params={"from": from_date, "to": to_date},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    return resp.json().get("earningsCalendar", [])


def get_quote(symbol: str) -> dict:
    resp = httpx.get(
        f"{FINNHUB_BASE}/quote",
        headers=FINNHUB_HEADERS,
        params={"symbol": symbol},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def get_company_news(symbol: str, from_date: str = None, to_date: str = None) -> list:
    if not from_date:
        from_date = (today_et() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = today_et().strftime("%Y-%m-%d")
    resp = httpx.get(
        f"{FINNHUB_BASE}/company-news",
        headers=FINNHUB_HEADERS,
        params={"symbol": symbol, "from": from_date, "to": to_date},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def get_sentiment_from_news(symbol: str) -> dict:
    articles = get_company_news(symbol)
    if not articles:
        return {"symbol": symbol, "score": 0.0, "article_count": 0, "headlines_sample": []}
    scores    = [score_headline(a.get("headline", "")) for a in articles]
    avg_score = round(sum(scores) / len(scores), 4)
    return {
        "symbol":           symbol,
        "score":            avg_score,
        "article_count":    len(articles),
        "headlines_sample": [a.get("headline") for a in articles[:3]]
    }


def search_symbols(query: str) -> list:
    """Search for symbols by company name or ticker."""
    resp = httpx.get(
        f"{FINNHUB_BASE}/search",
        headers=FINNHUB_HEADERS,
        params={"q": query},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    results = resp.json().get("result", [])
    filtered = [
        r for r in results
        if r.get("type") == "Common Stock" and "." not in r.get("symbol", "")
    ]
    return filtered[:6]


# ─── ALPHA VANTAGE FUNCTIONS (historical daily + intraday) ───────────────────

def get_historical_candles(symbol: str, days: int = 60) -> list:
    """
    Fetch historical daily candles from Alpha Vantage.
    Uses TIME_SERIES_DAILY — returns up to 100 trading days by default.
    Timestamps are ET dates (market close time in ET).
    """
    resp = httpx.get(
        AV_BASE,
        params={
            "function":   "TIME_SERIES_DAILY",
            "symbol":     symbol,
            "outputsize": "compact",   # last 100 trading days
            "apikey":     AV_KEY,
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    time_series = data.get("Time Series (Daily)")
    if not time_series:
        print(f"[AV] No daily data for {symbol}: {data.get('Note') or data.get('Information') or 'unknown error'}")
        return []

    candles = []
    cutoff  = today_et() - timedelta(days=days)

    for date_str, values in sorted(time_series.items()):
        try:
            # Parse as ET date at market close (16:00 ET)
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=16, minute=0, second=0)
            if ts >= cutoff.replace(tzinfo=None):
                candles.append({
                    "timestamp": ts,
                    "price":     round(float(values["4. close"]), 2)
                })
        except Exception:
            continue

    print(f"[AV] {symbol}: {len(candles)} daily candles fetched")
    return candles


def get_intraday_candles(symbol: str) -> list:
    """
    Fetch intraday 60-min candles from Alpha Vantage for the last trading day.
    Always uses the most recent weekday in ET so weekends/after-hours
    never produce wrong dates.
    Only returns candles within regular market hours: 9:30 AM - 4:00 PM ET.
    """
    trading_day = last_trading_day_et()
    trading_day_str = trading_day.strftime("%Y-%m-%d")

    resp = httpx.get(
        AV_BASE,
        params={
            "function":    "TIME_SERIES_INTRADAY",
            "symbol":      symbol,
            "interval":    "60min",
            "outputsize":  "full",    # get full day not just last 100 points
            "apikey":      AV_KEY,
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    time_series = data.get("Time Series (60min)")
    if not time_series:
        print(f"[AV] No intraday data for {symbol}: {data.get('Note') or data.get('Information') or 'unknown error'}")
        return []

    candles = []
    for dt_str, values in sorted(time_series.items()):
        try:
            # Alpha Vantage returns ET timestamps natively
            ts = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")

            # Only keep the last trading day
            if ts.strftime("%Y-%m-%d") != trading_day_str:
                continue

            # Only regular market hours: 9:30 AM to 4:00 PM ET
            if ts.hour > 9 or (ts.hour == 9 and ts.minute >= 30):
                if ts.hour < 16:
                    candles.append({
                        "timestamp": ts,
                        "price":     round(float(values["4. close"]), 2)
                    })
        except Exception:
            continue

    print(f"[AV] {symbol}: {len(candles)} intraday candles for {trading_day_str}")
    return candles
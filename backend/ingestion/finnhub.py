import httpx
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://finnhub.io/api/v1"
API_KEY  = "d6ti17pr01qhkb449fq0d6ti17pr01qhkb449fqg"

HEADERS  = {"X-Finnhub-Token": API_KEY}
TIMEOUT  = httpx.Timeout(30.0, connect=10.0)

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


def get_earnings_calendar(from_date: str = None, to_date: str = None) -> list:
    if not from_date:
        from_date = datetime.today().strftime("%Y-%m-%d")
    if not to_date:
        to_date = (datetime.today() + timedelta(days=14)).strftime("%Y-%m-%d")
    resp = httpx.get(
        f"{BASE_URL}/calendar/earnings",
        headers=HEADERS,
        params={"from": from_date, "to": to_date},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    return resp.json().get("earningsCalendar", [])


def get_quote(symbol: str) -> dict:
    resp = httpx.get(
        f"{BASE_URL}/quote",
        headers=HEADERS,
        params={"symbol": symbol},
        timeout=TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def get_company_news(symbol: str, from_date: str = None, to_date: str = None) -> list:
    if not from_date:
        from_date = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    if not to_date:
        to_date = datetime.today().strftime("%Y-%m-%d")
    resp = httpx.get(
        f"{BASE_URL}/company-news",
        headers=HEADERS,
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
        f"{BASE_URL}/search",
        headers=HEADERS,
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


def get_historical_candles(symbol: str, days: int = 60) -> list:
    """
    Fetch historical daily candles from Finnhub.
    Falls back to weekly if daily returns nothing.
    Returns list of {timestamp, price} dicts.
    """
    to_ts   = int(datetime.today().timestamp())
    from_ts = int((datetime.today() - timedelta(days=days)).timestamp())

    # Try daily first
    resp = httpx.get(
        f"{BASE_URL}/stock/candle",
        headers=HEADERS,
        params={
            "symbol":     symbol,
            "resolution": "D",
            "from":       from_ts,
            "to":         to_ts
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    if data.get("s") == "ok" and data.get("c"):
        return [
            {"timestamp": datetime.utcfromtimestamp(t), "price": c}
            for t, c in zip(data["t"], data["c"])
        ]

    # Fall back to weekly if daily fails
    resp = httpx.get(
        f"{BASE_URL}/stock/candle",
        headers=HEADERS,
        params={
            "symbol":     symbol,
            "resolution": "W",
            "from":       from_ts,
            "to":         to_ts
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    if data.get("s") == "ok" and data.get("c"):
        return [
            {"timestamp": datetime.utcfromtimestamp(t), "price": c}
            for t, c in zip(data["t"], data["c"])
        ]

    return []


def get_intraday_candles(symbol: str) -> list:
    """
    Fetch today's intraday 1-hour candles from market open (9:30 AM ET)
    to now. Works for both hardcoded and search bar stocks.

    Uses a 2-day window to handle UTC rollover — e.g. after 7 PM ET
    (midnight UTC) today's ET session is still within the last 24hrs UTC
    but the date has already flipped, so we always fetch from 2 days ago
    and filter to only keep candles from today's ET session.

    Market open  = 14:30 UTC (9:30 AM ET)
    Market close = 21:00 UTC (4:00 PM ET)
    """
    now_utc = datetime.utcnow()

    # Always fetch a 2-day window to avoid missing today's session
    # due to UTC/ET timezone differences
    to_ts   = int(now_utc.timestamp())
    from_ts = int((now_utc - timedelta(days=2)).timestamp())

    resp = httpx.get(
        f"{BASE_URL}/stock/candle",
        headers=HEADERS,
        params={
            "symbol":     symbol,
            "resolution": "60",   # 1-hour candles
            "from":       from_ts,
            "to":         to_ts
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    if not (data.get("s") == "ok" and data.get("c")):
        return []

    candles = [
        {"timestamp": datetime.utcfromtimestamp(t), "price": c}
        for t, c in zip(data["t"], data["c"])
    ]

    # Filter to only today's ET session:
    # Market open = 14:30 UTC, market close = 21:00 UTC
    # Use ET date: if UTC hour < 5, we're still on "yesterday ET"
    # so the trading day started at 14:30 UTC of the previous UTC day.
    if now_utc.hour < 5:
        # After midnight UTC but before 5 AM UTC = still previous ET day's session
        session_start = now_utc.replace(
            hour=14, minute=30, second=0, microsecond=0
        ) - timedelta(days=1)
    else:
        session_start = now_utc.replace(
            hour=14, minute=30, second=0, microsecond=0
        )

    session_end = session_start + timedelta(hours=6, minutes=30)  # 4:00 PM ET

    # Keep candles within today's session window
    today_candles = [
        c for c in candles
        if session_start <= c["timestamp"] <= session_end
    ]

    # If we got session candles, return those; otherwise return all fetched candles
    # (handles pre-market or extended hours edge cases)
    return today_candles if today_candles else candles
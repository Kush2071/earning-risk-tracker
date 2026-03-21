import httpx
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# Finnhub — quotes, news, search, earnings
FINNHUB_BASE    = "https://finnhub.io/api/v1"
FINNHUB_KEY     = "d6ti17pr01qhkb449fq0d6ti17pr01qhkb449fqg"
FINNHUB_HEADERS = {"X-Finnhub-Token": FINNHUB_KEY}

# Alpha Vantage — historical daily candles (5D, 1M charts)
AV_BASE = "https://www.alphavantage.co/query"
AV_KEY  = "NTMC4940Y5O7O5AV"

# Twelve Data — intraday candles (1D chart)
TD_BASE = "https://api.twelvedata.com"
TD_KEY  = "8a4c680298544837b76b9a73b03a1286"

TIMEOUT = httpx.Timeout(30.0, connect=10.0)

# Use ET everywhere
ET = ZoneInfo("America/New_York")

# VADER analyzer — loaded once at module level
_vader = None


def get_vader():
    """Lazy-load VADER analyzer on first use."""
    global _vader
    if _vader is not None:
        return _vader
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        _vader = SentimentIntensityAnalyzer()
        print("[VADER] Sentiment analyzer loaded")
        return _vader
    except Exception as e:
        print(f"[VADER] Failed to load: {e}")
        return None


def score_headline(headline: str) -> float:
    """
    Score a headline using VADER sentiment analysis.
    VADER understands negation ('not good' = negative),
    intensifiers ('very strong' = more positive), punctuation,
    and financial language better than keyword matching.
    Returns a float from -1.0 (very bearish) to +1.0 (very bullish).
    Falls back to keyword matching if VADER unavailable.
    """
    analyzer = get_vader()
    if analyzer:
        try:
            scores = analyzer.polarity_scores(headline)
            return round(scores["compound"], 4)
        except Exception:
            pass
    # Fallback to keyword matching
    return _score_keywords(headline)


def _score_keywords(headline: str) -> float:
    """Original keyword fallback — only used if VADER fails."""
    BULLISH = [
        "beat", "surpass", "record", "growth", "upgrade", "strong", "profit",
        "rise", "gain", "positive", "exceed", "outperform", "rally", "surge",
        "soar", "jump", "climb", "boost", "expand", "innovation", "launch",
        "partner", "deal", "win", "bullish", "opportunity", "demand", "revenue",
        "autonomous", "ai", "electric", "ev", "adoption", "milestone", "delivery",
        "breakthrough", "invest", "buy", "upside", "momentum", "recovery"
    ]
    BEARISH = [
        "miss", "below", "weak", "downgrade", "loss", "decline", "fall", "cut",
        "disappoint", "risk", "concern", "lawsuit", "recall", "investigation",
        "probe", "fine", "penalty", "short", "bearish", "sell", "crash", "drop",
        "plunge", "slump", "layoff", "delay", "competition", "rival", "pressure",
        "warning", "deficit", "debt", "volatile", "uncertain", "headwind", "slow"
    ]
    text  = headline.lower()
    bulls = sum(1 for w in BULLISH if w in text)
    bears = sum(1 for w in BEARISH if w in text)
    total = bulls + bears
    if total == 0:
        return 0.0
    return round((bulls - bears) / total, 4)


def now_et() -> datetime:
    return datetime.now(ET)


def today_et() -> datetime:
    n = now_et()
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


def last_trading_day_et() -> datetime:
    """Returns most recent weekday in ET. Sat→Fri, Sun→Fri, weekday→today."""
    n = now_et()
    weekday = n.weekday()
    if weekday == 5:
        return (n - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif weekday == 6:
        return (n - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
    return n.replace(hour=0, minute=0, second=0, microsecond=0)


# ─── FINNHUB — quotes, news, search, earnings ────────────────────────────────

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
    """
    Fetch news headlines and score them using VADER.
    Returns average sentiment score across all articles.
    """
    articles = get_company_news(symbol)
    if not articles:
        return {"symbol": symbol, "score": 0.0, "article_count": 0, "headlines_sample": []}

    scores = [score_headline(a.get("headline", "")) for a in articles if a.get("headline")]
    avg_score = round(sum(scores) / len(scores), 4) if scores else 0.0

    return {
        "symbol":           symbol,
        "score":            avg_score,
        "article_count":    len(articles),
        "headlines_sample": [a.get("headline") for a in articles[:3]]
    }


def search_symbols(query: str) -> list:
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


# ─── ALPHA VANTAGE — historical daily candles (5D, 1M charts) ────────────────

def get_historical_candles(symbol: str, days: int = 60) -> list:
    """
    Fetch historical daily closing prices from Alpha Vantage.
    Used for 5D and 1M charts.
    """
    resp = httpx.get(
        AV_BASE,
        params={
            "function":   "TIME_SERIES_DAILY",
            "symbol":     symbol,
            "outputsize": "compact",
            "apikey":     AV_KEY,
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    time_series = data.get("Time Series (Daily)")
    if not time_series:
        print(f"[AV] No daily data for {symbol}: {data.get('Note') or data.get('Information') or 'unknown'}")
        return []

    candles = []
    cutoff  = today_et() - timedelta(days=days)

    for date_str, values in sorted(time_series.items()):
        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(hour=16, minute=0, second=0)
            if ts >= cutoff.replace(tzinfo=None):
                candles.append({
                    "timestamp": ts,
                    "price":     round(float(values["4. close"]), 2)
                })
        except Exception:
            continue

    print(f"[AV] {symbol}: {len(candles)} daily candles")
    return candles


# ─── TWELVE DATA — intraday candles (1D chart) ───────────────────────────────

def get_intraday_candles(symbol: str) -> list:
    """
    Fetch intraday 1-hour candles from Twelve Data.
    Always fetches the last trading day (Sat/Sun → Friday).
    Only returns regular market hours: 9:30 AM - 4:00 PM ET.
    """
    trading_day     = last_trading_day_et()
    trading_day_str = trading_day.strftime("%Y-%m-%d")

    resp = httpx.get(
        f"{TD_BASE}/time_series",
        params={
            "symbol":     symbol,
            "interval":   "1h",
            "outputsize": 30,
            "timezone":   "America/New_York",
            "apikey":     TD_KEY,
        },
        timeout=TIMEOUT
    )
    data = resp.json()

    if data.get("status") == "error":
        print(f"[TD] Error for {symbol}: {data.get('message')}")
        return []

    values = data.get("values", [])
    if not values:
        print(f"[TD] No intraday data for {symbol}")
        return []

    candles = []
    for item in values:
        try:
            ts = datetime.strptime(item["datetime"], "%Y-%m-%d %H:%M:%S")
            if ts.strftime("%Y-%m-%d") != trading_day_str:
                continue
            if ts.hour > 9 or (ts.hour == 9 and ts.minute >= 30):
                if ts.hour < 16:
                    candles.append({
                        "timestamp": ts,
                        "price":     round(float(item["close"]), 2)
                    })
        except Exception:
            continue

    candles.sort(key=lambda x: x["timestamp"])
    print(f"[TD] {symbol}: {len(candles)} intraday candles for {trading_day_str}")
    return candles
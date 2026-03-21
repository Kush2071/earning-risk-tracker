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

# FinBERT model — loaded once at module level so it's cached across requests
# Uses ProsusAI/finbert which is specifically trained on financial news
_finbert_pipeline = None


def get_finbert():
    """
    Lazy-load FinBERT pipeline on first use.
    Downloads ~500MB on first run, cached locally after that.
    Falls back to VADER if transformers not available.
    """
    global _finbert_pipeline
    if _finbert_pipeline is not None:
        return _finbert_pipeline
    try:
        from transformers import pipeline
        print("[FinBERT] Loading model ProsusAI/finbert...")
        _finbert_pipeline = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            tokenizer="ProsusAI/finbert",
            top_k=None,          # return all 3 label scores
            truncation=True,
            max_length=512,
        )
        print("[FinBERT] Model loaded successfully")
        return _finbert_pipeline
    except Exception as e:
        print(f"[FinBERT] Failed to load: {e} — falling back to VADER")
        return None


def score_headline_finbert(headline: str) -> float:
    """
    Score a headline using FinBERT.
    Returns a float from -1.0 (very bearish) to +1.0 (very bullish).
    FinBERT returns: positive, negative, neutral labels with probabilities.
    Score = positive_prob - negative_prob
    """
    pipe = get_finbert()
    if pipe is None:
        return score_headline_vader(headline)

    try:
        results = pipe(headline)
        # results is a list of list of dicts: [[{label, score}, ...]]
        scores_list = results[0] if isinstance(results[0], list) else results
        label_map = {r["label"].lower(): r["score"] for r in scores_list}
        positive = label_map.get("positive", 0.0)
        negative = label_map.get("negative", 0.0)
        # Score range: -1 (all negative) to +1 (all positive)
        return round(positive - negative, 4)
    except Exception as e:
        print(f"[FinBERT] Inference error: {e}")
        return score_headline_vader(headline)


def score_headline_vader(headline: str) -> float:
    """
    Fallback sentiment scorer using VADER.
    Better than keyword matching — understands negation and intensifiers.
    """
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(headline)
        return round(scores["compound"], 4)  # -1 to +1
    except Exception:
        return score_headline_keywords(headline)


def score_headline_keywords(headline: str) -> float:
    """
    Last-resort fallback: original keyword matching.
    Only used if both FinBERT and VADER fail.
    """
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


def score_headline(headline: str) -> float:
    """
    Main entry point for sentiment scoring.
    Uses FinBERT → VADER → keywords as fallback chain.
    """
    return score_headline_finbert(headline)


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
    Fetch news headlines and score them using FinBERT.
    Returns average sentiment score across all articles.
    """
    articles = get_company_news(symbol)
    if not articles:
        return {"symbol": symbol, "score": 0.0, "article_count": 0, "headlines_sample": []}

    scores = []
    for a in articles:
        headline = a.get("headline", "")
        if headline:
            scores.append(score_headline(headline))

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
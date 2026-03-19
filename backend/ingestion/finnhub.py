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
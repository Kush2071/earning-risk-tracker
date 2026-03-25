from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session
from db.models import EarningsEvent, PriceSnapshot, SentimentRecord
from ingestion.finnhub import (
    get_earnings_calendar,
    get_quote,
    get_sentiment_from_news
)

ET = ZoneInfo("America/New_York")


def now_et() -> datetime:
    """Current time in ET as naive datetime for DB storage."""
    return datetime.now(ET).replace(tzinfo=None)


def ingest_earnings_calendar(db: Session):
    events  = get_earnings_calendar()
    added   = 0
    updated = 0
    for e in events:
        symbol = e.get("symbol")
        if not symbol:
            continue
        report_date = None
        if e.get("date"):
            try:
                report_date = datetime.strptime(e["date"], "%Y-%m-%d")
            except ValueError:
                continue
        existing = db.query(EarningsEvent).filter_by(
            symbol=symbol,
            report_date=report_date
        ).first()
        if existing:
            if e.get("epsActual") is not None:
                existing.eps_actual = e["epsActual"]
                if existing.eps_estimate and existing.eps_estimate != 0:
                    existing.surprise_pct = round(
                        (e["epsActual"] - existing.eps_estimate)
                        / abs(existing.eps_estimate) * 100, 4
                    )
                updated += 1
        else:
            eps_est    = e.get("epsEstimate")
            eps_actual = e.get("epsActual")
            surprise   = None
            if eps_est is not None and eps_actual is not None and eps_est != 0:
                surprise = round((eps_actual - eps_est) / abs(eps_est) * 100, 4)
            record = EarningsEvent(
                symbol=symbol,
                company_name=e.get("company", ""),
                report_date=report_date,
                eps_estimate=eps_est,
                eps_actual=eps_actual,
                surprise_pct=surprise,
                asset_class="equity"
            )
            db.add(record)
            added += 1
    db.commit()


def ingest_price_snapshot(db: Session, symbol: str, asset_class: str = "equity"):
    try:
        quote = get_quote(symbol)
        price = quote.get("c")
        if not price or price == 0:
            return
        snapshot = PriceSnapshot(
            symbol=symbol,
            asset_class=asset_class,
            price=price,
            volume=None,
            timestamp=now_et(),
            is_delayed=True
        )
        db.add(snapshot)
        db.commit()
    except Exception as ex:
        db.rollback()


def ingest_sentiment(db: Session, symbol: str):
    try:
        result = get_sentiment_from_news(symbol)
        score  = result.get("score")
        if score is None:
            return
        top_headline = result.get("headlines_sample", [None])[0]
        record = SentimentRecord(
            symbol=symbol,
            score=score,
            headline=top_headline,
            source="vader-sentiment",
            timestamp=now_et()
        )
        db.add(record)
        db.commit()
    except Exception as ex:
        db.rollback()


def run_full_ingest(db: Session, watchlist: list):
    ingest_earnings_calendar(db)
    for symbol in watchlist:
        ingest_price_snapshot(db, symbol)
    for symbol in watchlist:
        ingest_sentiment(db, symbol)
from datetime import datetime
from sqlalchemy.orm import Session
from db.models import EarningsEvent, PriceSnapshot, SentimentRecord
from ingestion.finnhub import (
    get_earnings_calendar,
    get_quote,
    get_sentiment_from_news
)


def ingest_earnings_calendar(db: Session):
    """Pull upcoming earnings and upsert into DB."""
    events = get_earnings_calendar()
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
    print(f"[Earnings] {added} added, {updated} updated")


def ingest_price_snapshot(db: Session, symbol: str, asset_class: str = "equity"):
    """Fetch delayed quote and store a snapshot."""
    try:
        quote = get_quote(symbol)
        price = quote.get("c")

        if not price or price == 0:
            print(f"[Price] {symbol}: no price returned, skipping")
            return

        snapshot = PriceSnapshot(
            symbol=symbol,
            asset_class=asset_class,
            price=price,
            volume=None,
            timestamp=datetime.utcnow(),
            is_delayed=True
        )
        db.add(snapshot)
        db.commit()
        print(f"[Price] {symbol} @ {price}")

    except Exception as ex:
        db.rollback()
        print(f"[Price] Failed for {symbol}: {ex}")


def ingest_sentiment(db: Session, symbol: str):
    """Score recent news headlines and store avg sentiment."""
    try:
        result = get_sentiment_from_news(symbol)
        score  = result.get("score")

        if score is None:
            print(f"[Sentiment] {symbol}: no score, skipping")
            return

        # Store one record per ingestion run with top headline as reference
        top_headline = result.get("headlines_sample", [None])[0]

        record = SentimentRecord(
            symbol=symbol,
            score=score,
            headline=top_headline,
            source="finnhub-news-keywords",
            timestamp=datetime.utcnow()
        )
        db.add(record)
        db.commit()
        print(f"[Sentiment] {symbol}: score={score} from {result['article_count']} articles")

    except Exception as ex:
        db.rollback()
        print(f"[Sentiment] Failed for {symbol}: {ex}")


def run_full_ingest(db: Session, watchlist: list):
    """Run all ingestion jobs for a given watchlist."""
    print("\n=== Starting full ingest ===")

    print("\n[1/3] Earnings calendar...")
    ingest_earnings_calendar(db)

    print("\n[2/3] Price snapshots...")
    for symbol in watchlist:
        ingest_price_snapshot(db, symbol)

    print("\n[3/3] Sentiment...")
    for symbol in watchlist:
        ingest_sentiment(db, symbol)

    print("\n=== Ingest complete ===")
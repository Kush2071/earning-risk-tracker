from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from db.database import SessionLocal
from db.models import PriceSnapshot
from ingestion.ingest import run_full_ingest
from ingestion.finnhub import get_quote
from risk.volatility import compute_and_store_risk
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

scheduler = BackgroundScheduler(timezone="America/Chicago")


def get_active_symbols():
    """Pull all unique symbols that have price data in the DB."""
    db = SessionLocal()
    try:
        rows = db.query(PriceSnapshot.symbol).distinct().all()
        symbols = [r.symbol for r in rows]
        return symbols if symbols else DEFAULT_WATCHLIST
    except:
        return DEFAULT_WATCHLIST
    finally:
        db.close()


def scheduled_ingest():
    """Job that runs every 15 minutes during market hours."""
    symbols = get_active_symbols()
    logger.info(f"[Scheduler] Running scheduled ingest for {symbols}...")
    db = SessionLocal()
    try:
        run_full_ingest(db, symbols)
    except Exception as e:
        logger.error(f"[Scheduler] Ingest failed: {e}")
    finally:
        db.close()


def store_closing_prices():
    """
    Runs at 4:05 PM CT every weekday.
    Fetches the final price of the day and marks it as the closing price.
    This becomes the next day's prev_close reference.
    """
    symbols = get_active_symbols()
    logger.info(f"[Scheduler] Storing closing prices for {symbols}...")
    db = SessionLocal()
    try:
        for symbol in symbols:
            try:
                quote = get_quote(symbol)
                price = quote.get("c")
                if price and price > 0:
                    snapshot = PriceSnapshot(
                        symbol=symbol,
                        asset_class="equity",
                        price=price,
                        volume=None,
                        timestamp=datetime.utcnow(),
                        is_delayed=False
                    )
                    db.add(snapshot)
                    logger.info(f"[Close] {symbol} closing price: ${price}")
            except Exception as e:
                logger.error(f"[Close] Failed for {symbol}: {e}")
        db.commit()

        # Recompute risk metrics with fresh closing data
        for symbol in symbols:
            try:
                compute_and_store_risk(db, symbol)
            except Exception as e:
                logger.error(f"[Close] Risk compute failed for {symbol}: {e}")

    except Exception as e:
        logger.error(f"[Scheduler] Closing price job failed: {e}")
    finally:
        db.close()


def start_scheduler():
    # Every 15 minutes during market hours Mon-Fri
    scheduler.add_job(
        scheduled_ingest,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour="9-15",
            minute="*/15",
            timezone="America/Chicago"
        ),
        id="market_hours_ingest",
        name="Ingest every 15 min during market hours",
        replace_existing=True
    )

    # At 4:05 PM CT every weekday — store closing prices
    scheduler.add_job(
        store_closing_prices,
        trigger=CronTrigger(
            day_of_week="mon-fri",
            hour=16,
            minute=5,
            timezone="America/Chicago"
        ),
        id="closing_price_store",
        name="Store closing prices at market close",
        replace_existing=True
    )

    scheduler.start()
    logger.info("[Scheduler] Started — market hours ingest + closing price job active")


def stop_scheduler():
    scheduler.shutdown()
    logger.info("[Scheduler] Stopped")
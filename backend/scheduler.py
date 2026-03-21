from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import SchedulerAlreadyRunningError
from db.database import SessionLocal
from db.models import PriceSnapshot
from ingestion.ingest import run_full_ingest
from ingestion.finnhub import get_quote, get_intraday_candles
from risk.volatility import compute_and_store_risk
from datetime import datetime
import logging
import atexit

logger = logging.getLogger(__name__)

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

scheduler = BackgroundScheduler(timezone="America/Chicago")
_scheduler_started = False


def get_active_symbols():
    """Pull all unique symbols that have price data in the DB."""
    db = SessionLocal()
    try:
        rows = db.query(PriceSnapshot.symbol).distinct().all()
        symbols = [r.symbol for r in rows]
        return symbols if symbols else DEFAULT_WATCHLIST
    except Exception as e:
        logger.error(f"Error getting active symbols: {e}")
        return DEFAULT_WATCHLIST
    finally:
        db.close()


def scheduled_ingest():
    def scheduled_ingest():
    """Job that runs every 15 minutes during market hours."""
    symbols = get_active_symbols()
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
    Fetches the final closing price AND full day's intraday candles
    for ALL active symbols so the 1D chart always shows the full session.
    This makes hardcoded stocks and search bar stocks behave identically.
    """
    symbols = get_active_symbols()
    logger.info(f"[Scheduler] Storing closing prices + intraday for {symbols}...")
    db = SessionLocal()
    try:
        for symbol in symbols:
            try:
                # 1. Store closing price snapshot
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

                # 2. Fetch and store full day's intraday candles.
                #    This ensures the 1D chart shows the complete trading
                #    session for ALL symbols — hardcoded and search bar alike.
                intraday = get_intraday_candles(symbol)
                if intraday:
                    for c in intraday:
                        existing = db.query(PriceSnapshot).filter_by(
                            symbol=symbol,
                            timestamp=c["timestamp"]
                        ).first()
                        if not existing:
                            db.add(PriceSnapshot(
                                symbol=symbol,
                                asset_class="equity",
                                price=round(c["price"], 2),
                                volume=None,
                                timestamp=c["timestamp"],
                                is_delayed=True
                            ))
                    logger.info(f"[Close] {symbol}: stored {len(intraday)} intraday candles")

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


def setup_scheduler_jobs():
    """Setup scheduler jobs (called only once)"""
    existing_job_ids = [job.id for job in scheduler.get_jobs()]

    # Every 15 minutes during market hours Mon-Fri (extended to 4 PM)
    if "market_hours_ingest" not in existing_job_ids:
        scheduler.add_job(
            scheduled_ingest,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour="9-16",
                minute="*/15",
                timezone="America/Chicago"
            ),
            id="market_hours_ingest",
            name="Ingest every 15 min during market hours",
            replace_existing=True
        )
        logger.info("[Scheduler] Added market hours ingest job")

    # At 4:05 PM CT every weekday — store closing prices + full intraday
    if "closing_price_store" not in existing_job_ids:
        scheduler.add_job(
            store_closing_prices,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=16,
                minute=5,
                timezone="America/Chicago"
            ),
            id="closing_price_store",
            name="Store closing prices + intraday at market close",
            replace_existing=True
        )
        logger.info("[Scheduler] Added closing price store job")

    # At 8:30 AM CT every weekday — pre-market morning ingest
    if "morning_ingest" not in existing_job_ids:
        scheduler.add_job(
            scheduled_ingest,
            trigger=CronTrigger(
                day_of_week="mon-fri",
                hour=8,
                minute=30,
                timezone="America/Chicago"
            ),
            id="morning_ingest",
            name="Pre-market morning ingest at 8:30 AM CT",
            replace_existing=True
        )
        logger.info("[Scheduler] Added morning ingest job")


def start_scheduler():
    """Start the scheduler if it's not already running"""
    global _scheduler_started

    if _scheduler_started:
        logger.info("[Scheduler] Scheduler already started, skipping")
        return

    try:
        if scheduler.running:
            logger.info("[Scheduler] Scheduler is already running")
            _scheduler_started = True
            return

        setup_scheduler_jobs()
        scheduler.start()
        _scheduler_started = True
        logger.info("[Scheduler] Started — market hours + closing + morning jobs active")

        atexit.register(stop_scheduler)

    except SchedulerAlreadyRunningError:
        logger.warning("[Scheduler] Scheduler already running (caught error)")
        _scheduler_started = True
    except Exception as e:
        logger.error(f"[Scheduler] Failed to start: {e}")
        raise


def stop_scheduler():
    """Stop the scheduler gracefully"""
    global _scheduler_started
    try:
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            _scheduler_started = False
            logger.info("[Scheduler] Stopped")
    except Exception as e:
        logger.error(f"[Scheduler] Error during shutdown: {e}")


def is_scheduler_running():
    """Check if scheduler is running"""
    return scheduler.running if scheduler else False


def restart_scheduler():
    """Restart the scheduler (useful for debugging)"""
    stop_scheduler()
    start_scheduler()
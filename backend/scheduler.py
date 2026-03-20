from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import SchedulerAlreadyRunningError
from db.database import SessionLocal
from db.models import PriceSnapshot
from ingestion.ingest import run_full_ingest
from ingestion.finnhub import get_quote
from risk.volatility import compute_and_store_risk
from datetime import datetime
import logging
import atexit

logger = logging.getLogger(__name__)

DEFAULT_WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

# Create scheduler at module level
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


def setup_scheduler_jobs():
    """Setup scheduler jobs (called only once)"""
    # Check if jobs are already added to avoid duplicates
    existing_job_ids = [job.id for job in scheduler.get_jobs()]
    
    # Every 15 minutes during market hours Mon-Fri
    if "market_hours_ingest" not in existing_job_ids:
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
        logger.info("[Scheduler] Added market hours ingest job")
    
    # At 4:05 PM CT every weekday — store closing prices
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
            name="Store closing prices at market close",
            replace_existing=True
        )
        logger.info("[Scheduler] Added closing price store job")


def start_scheduler():
    """Start the scheduler if it's not already running"""
    global _scheduler_started
    
    # Check if scheduler is already running
    if _scheduler_started:
        logger.info("[Scheduler] Scheduler already started, skipping")
        return
    
    try:
        # Check if scheduler is already running
        if scheduler.running:
            logger.info("[Scheduler] Scheduler is already running")
            _scheduler_started = True
            return
        
        # Setup jobs (this will only add jobs if they don't exist)
        setup_scheduler_jobs()
        
        # Start the scheduler
        scheduler.start()
        _scheduler_started = True
        logger.info("[Scheduler] Started — market hours ingest + closing price job active")
        
        # Register cleanup on application exit
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


# Optional: Add a health check function
def is_scheduler_running():
    """Check if scheduler is running"""
    return scheduler.running if scheduler else False


# Optional: Add a function to restart scheduler if needed
def restart_scheduler():
    """Restart the scheduler (useful for debugging)"""
    stop_scheduler()
    start_scheduler()
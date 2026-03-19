from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from db.database import SessionLocal
from ingestion.ingest import run_full_ingest
import logging

logger = logging.getLogger(__name__)

WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

scheduler = BackgroundScheduler()


def scheduled_ingest():
    """Job that runs every 15 minutes automatically."""
    logger.info("[Scheduler] Running scheduled ingest...")
    db = SessionLocal()
    try:
        run_full_ingest(db, WATCHLIST)
    except Exception as e:
        logger.error(f"[Scheduler] Ingest failed: {e}")
    finally:
        db.close()


def start_scheduler():
    scheduler.add_job(
        scheduled_ingest,
        trigger=IntervalTrigger(minutes=15),
        id="full_ingest",
        name="Full data ingest every 15 minutes",
        replace_existing=True
    )
    scheduler.start()
    logger.info("[Scheduler] Started — running every 15 minutes")


def stop_scheduler():
    scheduler.shutdown()
    logger.info("[Scheduler] Stopped")
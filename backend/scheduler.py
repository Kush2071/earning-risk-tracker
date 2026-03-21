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
    symbols = get_active_symbols()
    db = SessionLocal()
    try:
        run_full_ingest(db, symbols)
    except Exception as e:
        logger.error(f"[Scheduler] Ingest failed: {e}")
    finally:
        db.close()


def store_closing_prices():
    symbols = get_active_symbols()
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
            except Exception as e:
                logger.error(f"[Close] Failed for {symbol}: {e}")

        db.commit()

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
    existing_job_ids = [job.id for job in scheduler.get_jobs()]

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


def start_scheduler():
    global _scheduler_started

    if _scheduler_started:
        return

    try:
        if scheduler.running:
            _scheduler_started = True
            return

        setup_scheduler_jobs()
        scheduler.start()
        _scheduler_started = True
        atexit.register(stop_scheduler)

    except SchedulerAlreadyRunningError:
        _scheduler_started = True
    except Exception as e:
        logger.error(f"[Scheduler] Failed to start: {e}")
        raise


def stop_scheduler():
    global _scheduler_started
    try:
        if scheduler and scheduler.running:
            scheduler.shutdown(wait=False)
            _scheduler_started = False
    except Exception as e:
        logger.error(f"[Scheduler] Error during shutdown: {e}")


def is_scheduler_running():
    return scheduler.running if scheduler else False


def restart_scheduler():
    stop_scheduler()
    start_scheduler()
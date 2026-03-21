import math
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from db.database import engine, Base, get_db
import db.models
from db.models import EarningsEvent, PriceSnapshot, SentimentRecord, RiskMetric
from ingestion.ingest import (
    ingest_earnings_calendar,
    ingest_price_snapshot,
    ingest_sentiment,
    run_full_ingest
)
from ingestion.finnhub import search_symbols, get_quote, get_historical_candles, get_intraday_candles
from risk.volatility import (
    compute_and_store_risk,
    calculate_returns,
    calculate_sharpe,
    calculate_expected_move,
    calculate_position_size,
)
from scheduler import start_scheduler, stop_scheduler

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Earnings Risk Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]
ET = ZoneInfo("America/New_York")

_startup_complete = False


def now_et():
    return datetime.now(ET).replace(tzinfo=None)


def get_active_symbols_list(db):
    rows = db.query(PriceSnapshot.symbol).distinct().all()
    symbols = [r.symbol for r in rows]
    return symbols if symbols else WATCHLIST


def store_intraday_for_symbol(db, symbol):
    """Fetch and store intraday candles for a symbol."""
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
        db.commit()
        print(f"[Intraday] {symbol}: {len(intraday)} candles stored")
    return intraday


@app.on_event("startup")
async def startup_event():
    global _startup_complete

    if _startup_complete:
        return

    start_scheduler()

    db = next(get_db())
    try:
        count = db.query(PriceSnapshot).count()
        if count == 0:
            print("[Startup] Empty DB — running initial ingest...")
            run_full_ingest(db, WATCHLIST)

        print("[Startup] Fetching intraday data...")
        for symbol in WATCHLIST:
            store_intraday_for_symbol(db, symbol)

        for symbol in WATCHLIST:
            compute_and_store_risk(db, symbol)

    except Exception as e:
        print(f"[Startup] Failed: {e}")
    finally:
        db.close()

    _startup_complete = True


@app.on_event("shutdown")
def shutdown_event():
    stop_scheduler()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/search")
def search(q: str = Query(...), db: Session = Depends(get_db)):
    try:
        results = search_symbols(q)
        return [{"symbol": r["symbol"], "name": r["description"]} for r in results]
    except Exception:
        return []


@app.get("/live-price/{symbol}")
def live_price(symbol: str, db: Session = Depends(get_db)):
    try:
        quote = get_quote(symbol.upper())
        price = quote.get("c")
        pc    = quote.get("pc")

        if price and price > 0:
            snapshot = PriceSnapshot(
                symbol=symbol.upper(),
                asset_class="equity",
                price=price,
                volume=None,
                timestamp=now_et(),
                is_delayed=True
            )
            db.add(snapshot)
            db.commit()

        return {
            "symbol":     symbol.upper(),
            "price":      price,
            "prev_close": pc,
            "change":     round(price - pc, 2) if price and pc else None,
            "change_pct": round((price - pc) / pc * 100, 2) if price and pc else None,
        }
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}


@app.post("/ingest/all")
def trigger_full_ingest(db: Session = Depends(get_db)):
    symbols = get_active_symbols_list(db)
    run_full_ingest(db, symbols)
    for symbol in symbols:
        store_intraday_for_symbol(db, symbol)
    return {"status": "done", "symbols": symbols}


@app.post("/ingest/earnings")
def trigger_earnings(db: Session = Depends(get_db)):
    ingest_earnings_calendar(db)
    return {"status": "done"}


@app.post("/ingest/prices")
def trigger_prices(db: Session = Depends(get_db)):
    symbols = get_active_symbols_list(db)
    for symbol in symbols:
        ingest_price_snapshot(db, symbol)
    return {"status": "done", "symbols": symbols}


@app.post("/ingest/sentiment")
def trigger_sentiment(db: Session = Depends(get_db)):
    symbols = get_active_symbols_list(db)
    for symbol in symbols:
        ingest_sentiment(db, symbol)
    return {"status": "done", "symbols": symbols}


@app.post("/ingest/risk")
def trigger_risk(db: Session = Depends(get_db)):
    symbols = get_active_symbols_list(db)
    for symbol in symbols:
        compute_and_store_risk(db, symbol)
    return {"status": "done", "symbols": symbols}


@app.post("/ingest/symbol/{symbol}")
def ingest_single_symbol(symbol: str, db: Session = Depends(get_db)):
    s = symbol.upper()

    ingest_price_snapshot(db, s)

    print(f"[Seed] Fetching real historical data for {s}...")
    candles = get_historical_candles(s, days=60)

    if candles:
        print(f"[Seed] Got {len(candles)} real candles for {s}")
        for c in candles:
            existing = db.query(PriceSnapshot).filter_by(
                symbol=s, timestamp=c["timestamp"]
            ).first()
            if not existing:
                db.add(PriceSnapshot(
                    symbol=s, asset_class="equity",
                    price=round(c["price"], 2), volume=None,
                    timestamp=c["timestamp"], is_delayed=True
                ))
        db.commit()
    else:
        print(f"[Seed] No Finnhub daily data for {s}")

    print(f"[Seed] Fetching intraday candles for {s}...")
    intraday = store_intraday_for_symbol(db, s)
    if not intraday:
        print(f"[Seed] No intraday data for {s} — will populate on next market open")

    compute_and_store_risk(db, s)
    ingest_sentiment(db, s)

    return {"status": "done", "symbol": s}


@app.post("/ingest/seed-history")
def seed_history(
    force: bool = Query(default=False),
    db: Session = Depends(get_db)
):
    symbols = get_active_symbols_list(db)
    seeded  = []

    for symbol in symbols:
        existing_count = db.query(PriceSnapshot).filter_by(symbol=symbol).count()

        if existing_count > 100 and not force:
            print(f"[Seed] {symbol} has {existing_count} snapshots, use force=true to reseed")
            continue

        db.query(PriceSnapshot).filter_by(symbol=symbol).delete()
        db.commit()
        print(f"[Seed] Cleared {existing_count} snapshots for {symbol}")

        candles = get_historical_candles(symbol, days=60)

        if candles:
            for c in candles:
                db.add(PriceSnapshot(
                    symbol=symbol, asset_class="equity",
                    price=round(c["price"], 2), volume=None,
                    timestamp=c["timestamp"], is_delayed=True
                ))
            db.commit()
        else:
            print(f"[Seed] No Finnhub data for {symbol}, skipping")
            seeded.append(f"{symbol}(skipped)")
            continue

        intraday = store_intraday_for_symbol(db, symbol)
        count_str = f"{len(candles)} daily"
        if intraday:
            count_str += f" + {len(intraday)} intraday"
        seeded.append(f"{symbol}({count_str})")

    for symbol in symbols:
        try:
            compute_and_store_risk(db, symbol)
        except Exception as e:
            print(f"[Seed] Risk failed for {symbol}: {e}")

    return {"status": "done", "seeded": seeded}


@app.post("/ingest/force-reseed/{symbol}")
def force_reseed_symbol(symbol: str, db: Session = Depends(get_db)):
    s = symbol.upper()

    count = db.query(PriceSnapshot).filter_by(symbol=s).count()
    db.query(PriceSnapshot).filter_by(symbol=s).delete()
    db.commit()

    candles = get_historical_candles(s, days=90)
    daily_count = 0

    if candles:
        for c in candles:
            db.add(PriceSnapshot(
                symbol=s, asset_class="equity",
                price=round(c["price"], 2), volume=None,
                timestamp=c["timestamp"], is_delayed=True
            ))
        db.commit()
        daily_count = len(candles)

    intraday = store_intraday_for_symbol(db, s)
    intraday_count = len(intraday) if intraday else 0

    ingest_price_snapshot(db, s)
    compute_and_store_risk(db, s)

    total = db.query(PriceSnapshot).filter_by(symbol=s).count()

    return {
        "symbol":           s,
        "deleted":          count,
        "daily_candles":    daily_count,
        "intraday_candles": intraday_count,
        "total_snapshots":  total,
        "finnhub_data":     daily_count > 0
    }


@app.get("/earnings")
def list_earnings(db: Session = Depends(get_db)):
    rows = db.query(EarningsEvent)\
             .order_by(EarningsEvent.report_date)\
             .limit(50).all()
    return [
        {
            "symbol":       r.symbol,
            "company":      r.company_name,
            "date":         str(r.report_date),
            "eps_estimate": r.eps_estimate,
            "eps_actual":   r.eps_actual,
            "surprise_pct": r.surprise_pct
        }
        for r in rows
    ]


@app.get("/prices/{symbol}")
def get_prices(
    symbol: str,
    limit: int = Query(default=200),
    db: Session = Depends(get_db)
):
    rows = db.query(PriceSnapshot)\
             .filter_by(symbol=symbol.upper())\
             .order_by(PriceSnapshot.timestamp.desc())\
             .limit(limit).all()
    return [{"price": r.price, "timestamp": str(r.timestamp)} for r in rows]


@app.get("/sentiment/{symbol}")
def get_sentiment(symbol: str, db: Session = Depends(get_db)):
    rows = db.query(SentimentRecord)\
             .filter_by(symbol=symbol.upper())\
             .order_by(SentimentRecord.timestamp.desc())\
             .limit(10).all()
    return [{"score": r.score, "headline": r.headline, "timestamp": str(r.timestamp)} for r in rows]


@app.get("/risk/{symbol}")
def get_risk(symbol: str, db: Session = Depends(get_db)):
    row = db.query(RiskMetric)\
            .filter_by(symbol=symbol.upper())\
            .order_by(RiskMetric.computed_at.desc())\
            .first()
    if not row:
        return {"symbol": symbol, "var_95": None, "volatility_30d": None, "beta": None, "sharpe": None, "alpha": None}

    price_rows = db.query(PriceSnapshot)\
                   .filter_by(symbol=symbol.upper())\
                   .order_by(PriceSnapshot.timestamp.asc()).all()
    prices  = [r.price for r in price_rows]
    returns = calculate_returns(prices) if len(prices) >= 3 else []
    sharpe  = calculate_sharpe(returns) if returns else None

    return {
        "symbol":         row.symbol,
        "var_95":         row.var_95,
        "volatility_30d": row.volatility_30d,
        "beta":           row.beta,
        "alpha":          row.alpha,
        "sharpe":         sharpe,
        "computed_at":    str(row.computed_at)
    }


@app.get("/risk")
def get_all_risk(
    symbols: str = Query(default=None),
    db: Session = Depends(get_db)
):
    target = symbols.split(",") if symbols else WATCHLIST
    results = []
    for symbol in target:
        symbol = symbol.strip().upper()
        row = db.query(RiskMetric)\
                .filter_by(symbol=symbol)\
                .order_by(RiskMetric.computed_at.desc())\
                .first()
        if not row:
            continue
        price_rows = db.query(PriceSnapshot)\
                       .filter_by(symbol=symbol)\
                       .order_by(PriceSnapshot.timestamp.asc()).all()
        prices  = [r.price for r in price_rows]
        returns = calculate_returns(prices) if len(prices) >= 3 else []
        sharpe  = calculate_sharpe(returns) if returns else None
        results.append({
            "symbol":         row.symbol,
            "var_95":         row.var_95,
            "volatility_30d": row.volatility_30d,
            "beta":           row.beta,
            "alpha":          row.alpha,
            "sharpe":         sharpe,
        })
    return results


@app.get("/expected-move/{symbol}")
def get_expected_move(
    symbol: str,
    days: int = Query(default=1),
    db: Session = Depends(get_db)
):
    row = db.query(RiskMetric)\
            .filter_by(symbol=symbol.upper())\
            .order_by(RiskMetric.computed_at.desc())\
            .first()
    price_row = db.query(PriceSnapshot)\
                  .filter_by(symbol=symbol.upper())\
                  .order_by(PriceSnapshot.timestamp.desc())\
                  .first()
    if not row or not price_row:
        return {"symbol": symbol, "error": "insufficient data"}

    move = calculate_expected_move(price_row.price, row.volatility_30d, days)
    return {
        "symbol":               symbol,
        "price":                price_row.price,
        "days":                 days,
        "expected_move_dollar": move["dollar"],
        "expected_move_pct":    move["pct"],
        "range_low":            round(price_row.price - move["dollar"], 2),
        "range_high":           round(price_row.price + move["dollar"], 2),
    }


@app.get("/position-size/{symbol}")
def get_position_size(
    symbol: str,
    portfolio: float = Query(default=100000),
    risk_pct: float  = Query(default=0.01),
    db: Session = Depends(get_db)
):
    row = db.query(RiskMetric)\
            .filter_by(symbol=symbol.upper())\
            .order_by(RiskMetric.computed_at.desc())\
            .first()
    price_row = db.query(PriceSnapshot)\
                  .filter_by(symbol=symbol.upper())\
                  .order_by(PriceSnapshot.timestamp.desc())\
                  .first()
    if not row or not price_row:
        return {"symbol": symbol, "error": "insufficient data"}

    sizing = calculate_position_size(portfolio, price_row.price, row.var_95, risk_pct)
    return {
        "symbol":           symbol,
        "portfolio_value":  portfolio,
        "risk_pct_input":   risk_pct * 100,
        "price":            price_row.price,
        "var_95_per_share": row.var_95,
        **sizing
    }


@app.get("/scheduler/status")
def scheduler_status():
    from scheduler import scheduler
    jobs = scheduler.get_jobs()
    return {
        "running": scheduler.running,
        "jobs": [{"id": j.id, "next_run": str(j.next_run_time)} for j in jobs]
    }


@app.post("/ingest/intraday")
def trigger_intraday(db: Session = Depends(get_db)):
    symbols = get_active_symbols_list(db)
    results = []
    for symbol in symbols:
        intraday = store_intraday_for_symbol(db, symbol)
        if intraday:
            results.append(f"{symbol}({len(intraday)} candles)")
    return {"status": "done", "results": results}
import math
import random
from datetime import datetime, timedelta
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

_startup_complete = False


def get_active_symbols_list(db):
    """Get all unique symbols that have price data."""
    rows = db.query(PriceSnapshot.symbol).distinct().all()
    symbols = [r.symbol for r in rows]
    return symbols if symbols else WATCHLIST


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

        # Always fetch intraday from market open on startup
        print("[Startup] Fetching intraday data from market open...")
        for symbol in WATCHLIST:
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
                print(f"[Startup] {symbol}: {len(intraday)} intraday candles loaded")

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
                timestamp=datetime.utcnow(),
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
    run_full_ingest(db, WATCHLIST)
    return {"status": "done", "symbols": WATCHLIST}


@app.post("/ingest/earnings")
def trigger_earnings(db: Session = Depends(get_db)):
    ingest_earnings_calendar(db)
    return {"status": "done"}


@app.post("/ingest/prices")
def trigger_prices(db: Session = Depends(get_db)):
    for symbol in WATCHLIST:
        ingest_price_snapshot(db, symbol)
    return {"status": "done", "symbols": WATCHLIST}


@app.post("/ingest/sentiment")
def trigger_sentiment(db: Session = Depends(get_db)):
    for symbol in WATCHLIST:
        ingest_sentiment(db, symbol)
    return {"status": "done", "symbols": WATCHLIST}


@app.post("/ingest/risk")
def trigger_risk(db: Session = Depends(get_db)):
    for symbol in WATCHLIST:
        compute_and_store_risk(db, symbol)
    return {"status": "done", "symbols": WATCHLIST}


@app.post("/ingest/symbol/{symbol}")
def ingest_single_symbol(symbol: str, db: Session = Depends(get_db)):
    """
    Add a new symbol — fetch real historical data from Finnhub,
    fall back to synthetic if Finnhub returns nothing.
    """
    s = symbol.upper()

    # Step 1: get current price
    ingest_price_snapshot(db, s)

    # Step 2: try to get real historical daily candles
    print(f"[Seed] Fetching real historical data for {s}...")
    candles = get_historical_candles(s, days=60)

    if candles:
        print(f"[Seed] Got {len(candles)} real candles for {s}")
        for c in candles:
            existing = db.query(PriceSnapshot).filter_by(
                symbol=s,
                timestamp=c["timestamp"]
            ).first()
            if not existing:
                db.add(PriceSnapshot(
                    symbol=s,
                    asset_class="equity",
                    price=round(c["price"], 2),
                    volume=None,
                    timestamp=c["timestamp"],
                    is_delayed=True
                ))
        db.commit()
    else:
        # Fallback to synthetic if Finnhub returns nothing
        print(f"[Seed] No Finnhub daily data for {s}, using synthetic history")
        price_row = db.query(PriceSnapshot).filter_by(symbol=s)\
                      .order_by(PriceSnapshot.timestamp.desc()).first()
        if price_row:
            current_price = price_row.price
            today = datetime.utcnow()
            for i in range(60):
                ret = random.gauss(0, 0.02)
                synthetic_price = current_price * math.exp(-ret * (60 - i) / 60)
                ts = today - timedelta(days=(60 - i))
                db.add(PriceSnapshot(
                    symbol=s, asset_class="equity",
                    price=round(synthetic_price, 2),
                    volume=None, timestamp=ts, is_delayed=True
                ))
            db.commit()

    # FIX 3: Always fetch today's intraday candles unconditionally,
    # regardless of whether daily candles were available.
    # Previously this was inside `if candles:` so new symbols with
    # rate-limited or missing daily data never got intraday data,
    # causing the 1D chart to show only a single dot at current time.
    print(f"[Seed] Fetching intraday candles for {s}...")
    intraday = get_intraday_candles(s)
    if intraday:
        for c in intraday:
            existing = db.query(PriceSnapshot).filter_by(
                symbol=s,
                timestamp=c["timestamp"]
            ).first()
            if not existing:
                db.add(PriceSnapshot(
                    symbol=s,
                    asset_class="equity",
                    price=round(c["price"], 2),
                    volume=None,
                    timestamp=c["timestamp"],
                    is_delayed=True
                ))
        db.commit()
        print(f"[Seed] Added {len(intraday)} intraday candles for {s}")
    else:
        print(f"[Seed] No intraday data available for {s} (market may be closed)")

    # Step 3: compute risk
    compute_and_store_risk(db, s)

    # Step 4: sentiment
    ingest_sentiment(db, s)

    return {"status": "done", "symbol": s}


@app.post("/ingest/seed-history")
def seed_history(
    force: bool = Query(default=False, description="Force delete and reseed all symbols"),
    db: Session = Depends(get_db)
):
    """Seed real historical prices for all active symbols."""
    symbols = get_active_symbols_list(db)
    seeded  = []

    for symbol in symbols:
        existing_count = db.query(PriceSnapshot)\
                           .filter_by(symbol=symbol).count()

        if existing_count > 100 and not force:
            print(f"[Seed] {symbol} has {existing_count} snapshots, use force=true to reseed")
            continue

        # Delete all existing price data for this symbol
        db.query(PriceSnapshot).filter_by(symbol=symbol).delete()
        db.commit()
        print(f"[Seed] Cleared {existing_count} snapshots for {symbol}")

        # Fetch real historical daily candles
        print(f"[Seed] Fetching real history for {symbol}...")
        candles = get_historical_candles(symbol, days=60)

        if candles:
            print(f"[Seed] Got {len(candles)} real candles for {symbol}")
            for c in candles:
                db.add(PriceSnapshot(
                    symbol=symbol,
                    asset_class="equity",
                    price=round(c["price"], 2),
                    volume=None,
                    timestamp=c["timestamp"],
                    is_delayed=True
                ))
            db.commit()
        else:
            print(f"[Seed] No Finnhub data for {symbol}, using synthetic")
            current_price = 100.0
            today = datetime.utcnow()
            for i in range(60):
                ret = random.gauss(0, 0.012)
                synthetic_price = current_price * math.exp(-ret * (60 - i) / 60)
                ts = today - timedelta(days=(60 - i))
                db.add(PriceSnapshot(
                    symbol=symbol, asset_class="equity",
                    price=round(synthetic_price, 2),
                    volume=None, timestamp=ts, is_delayed=True
                ))
            db.commit()
            seeded.append(f"{symbol}(synthetic)")
            continue

        # Always fetch intraday regardless of whether daily candles succeeded
        intraday = get_intraday_candles(symbol)
        if intraday:
            for c in intraday:
                db.add(PriceSnapshot(
                    symbol=symbol,
                    asset_class="equity",
                    price=round(c["price"], 2),
                    volume=None,
                    timestamp=c["timestamp"],
                    is_delayed=True
                ))
            db.commit()
            print(f"[Seed] Added {len(intraday)} intraday candles for {symbol}")
            seeded.append(f"{symbol}({len(candles)} real + {len(intraday)} intraday)")
        else:
            seeded.append(f"{symbol}({len(candles)} real)")

    # Recompute risk for all
    for symbol in symbols:
        try:
            compute_and_store_risk(db, symbol)
        except Exception as e:
            print(f"[Seed] Risk failed for {symbol}: {e}")

    return {"status": "done", "seeded": seeded}


@app.post("/ingest/force-reseed/{symbol}")
def force_reseed_symbol(symbol: str, db: Session = Depends(get_db)):
    """
    Force delete ALL price data for a symbol and fetch fresh
    real historical data from Finnhub.
    """
    s = symbol.upper()

    # Count and delete everything
    count = db.query(PriceSnapshot).filter_by(symbol=s).count()
    db.query(PriceSnapshot).filter_by(symbol=s).delete()
    db.commit()
    print(f"[ForceReseed] Deleted {count} snapshots for {s}")

    # Fetch real daily candles from Finnhub
    candles = get_historical_candles(s, days=90)
    daily_count = 0

    if candles:
        for c in candles:
            db.add(PriceSnapshot(
                symbol=s,
                asset_class="equity",
                price=round(c["price"], 2),
                volume=None,
                timestamp=c["timestamp"],
                is_delayed=True
            ))
        db.commit()
        daily_count = len(candles)
        print(f"[ForceReseed] Added {daily_count} real daily candles for {s}")
    else:
        print(f"[ForceReseed] No Finnhub daily data for {s}")

    # Fetch today's intraday candles (always, unconditionally)
    intraday = get_intraday_candles(s)
    intraday_count = 0

    if intraday:
        for c in intraday:
            db.add(PriceSnapshot(
                symbol=s,
                asset_class="equity",
                price=round(c["price"], 2),
                volume=None,
                timestamp=c["timestamp"],
                is_delayed=True
            ))
        db.commit()
        intraday_count = len(intraday)
        print(f"[ForceReseed] Added {intraday_count} intraday candles for {s}")

    # Also get current live price
    ingest_price_snapshot(db, s)

    # Recompute risk
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
        return {"symbol": symbol, "var_95": None, "volatility_30d": None, "beta": None, "sharpe": None}

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
    """Fetch today's intraday candles from market open for all symbols."""
    symbols = get_active_symbols_list(db)
    results = []
    for symbol in symbols:
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
            results.append(f"{symbol}({len(intraday)} candles)")
            print(f"[Intraday] {symbol}: {len(intraday)} candles")
    return {"status": "done", "results": results}
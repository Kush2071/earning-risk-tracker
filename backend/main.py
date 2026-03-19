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
from ingestion.finnhub import search_symbols, get_quote
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


@app.on_event("startup")
def startup_event():
    start_scheduler()
    # Auto-seed data on first boot if DB is empty
    db = next(get_db())
    try:
        count = db.query(PriceSnapshot).count()
        if count == 0:
            print("[Startup] Empty DB detected — running initial ingest...")
            run_full_ingest(db, WATCHLIST)
            for symbol in WATCHLIST:
                compute_and_store_risk(db, symbol)
            print("[Startup] Initial ingest complete")
    except Exception as e:
        print(f"[Startup] Ingest failed: {e}")
    finally:
        db.close()


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
    except Exception as e:
        return []


@app.get("/live-price/{symbol}")
def live_price(symbol: str, db: Session = Depends(get_db)):
    try:
        quote = get_quote(symbol.upper())
        price = quote.get("c")
        prev  = quote.get("pc")

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
            "prev_close": prev,
            "change":     round(price - prev, 2) if price and prev else None,
            "change_pct": round((price - prev) / prev * 100, 2) if price and prev else None,
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
    s = symbol.upper()
    ingest_price_snapshot(db, s)

    count = db.query(PriceSnapshot).filter_by(symbol=s).count()
    if count < 10:
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
            print(f"[Seed] Seeded 60 synthetic prices for {s}")

    compute_and_store_risk(db, s)
    ingest_sentiment(db, s)
    return {"status": "done", "symbol": s}


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
def get_prices(symbol: str, db: Session = Depends(get_db)):
    rows = db.query(PriceSnapshot)\
             .filter_by(symbol=symbol.upper())\
             .order_by(PriceSnapshot.timestamp.desc())\
             .limit(60).all()
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
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.sql import func
from db.database import Base


class EarningsEvent(Base):
    __tablename__ = "earnings_events"

    id              = Column(Integer, primary_key=True, index=True)
    symbol          = Column(String, index=True)
    company_name    = Column(String)
    report_date     = Column(DateTime)
    eps_estimate    = Column(Float, nullable=True)
    eps_actual      = Column(Float, nullable=True)
    surprise_pct    = Column(Float, nullable=True)
    asset_class     = Column(String, default="equity")
    created_at      = Column(DateTime, server_default=func.now())
    updated_at      = Column(DateTime, onupdate=func.now())


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id          = Column(Integer, primary_key=True, index=True)
    symbol      = Column(String, index=True)
    asset_class = Column(String)
    price       = Column(Float)
    volume      = Column(Float, nullable=True)
    timestamp   = Column(DateTime, index=True)
    is_delayed  = Column(Boolean, default=True)


class SentimentRecord(Base):
    __tablename__ = "sentiment_records"

    id          = Column(Integer, primary_key=True, index=True)
    symbol      = Column(String, index=True)
    score       = Column(Float)
    headline    = Column(String, nullable=True)
    source      = Column(String, nullable=True)
    timestamp   = Column(DateTime, index=True)


class RiskMetric(Base):
    __tablename__ = "risk_metrics"

    id              = Column(Integer, primary_key=True, index=True)
    symbol          = Column(String, index=True)
    var_95          = Column(Float, nullable=True)
    volatility_30d  = Column(Float, nullable=True)
    beta            = Column(Float, nullable=True)
    computed_at     = Column(DateTime, server_default=func.now())
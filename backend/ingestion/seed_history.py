import sys
import os
import random
import math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from db.database import SessionLocal
from db.models import PriceSnapshot

WATCHLIST = {
    "AAPL":  249.94,
    "MSFT":  391.79,
    "NVDA":  180.40,
    "GOOGL": 307.69,
    "AMZN":  209.87,
}


def generate_prices(current_price, days=60, daily_vol=0.02):
    """Walk backwards from current price using random returns."""
    prices = [current_price]
    for _ in range(days - 1):
        ret = random.gauss(0, daily_vol)
        prices.append(prices[-1] * math.exp(-ret))
    return list(reversed(prices))


def seed_historical_prices():
    db = SessionLocal()

    for symbol, current_price in WATCHLIST.items():
        print(f"Seeding {symbol}...")

        # Check if already seeded
        existing = db.query(PriceSnapshot).filter_by(symbol=symbol).count()
        if existing > 10:
            print(f"  Already has {existing} snapshots, skipping")
            continue

        prices = generate_prices(current_price, days=60)
        today  = datetime.utcnow()

        for i, price in enumerate(prices):
            ts = today - timedelta(days=(60 - i))
            snapshot = PriceSnapshot(
                symbol=symbol,
                asset_class="equity",
                price=round(price, 2),
                volume=None,
                timestamp=ts,
                is_delayed=True
            )
            db.add(snapshot)

        db.commit()
        print(f"  Added 60 synthetic daily prices for {symbol}")

    db.close()
    print("\nSeeding complete.")


if __name__ == "__main__":
    seed_historical_prices()
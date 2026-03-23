from __future__ import annotations

import csv
import json
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, Iterable, List


# Number of recent prices used for rolling mean calculation
ROLLING_WINDOW = 3

# Percentage threshold to flag price anomalies (sudden jumps)
ANOMALY_THRESHOLD_PCT = 0.8


@dataclass
class SymbolState:
    """Maintains aggregated state for a single symbol during processing."""
    
    total_volume: int = 0                  # Total traded volume
    total_notional: float = 0.0            # Sum of (price * volume) for VWAP
    rolling_prices: Deque[float] = field(default_factory=lambda: deque(maxlen=ROLLING_WINDOW))
    rolling_mean_last: float = 0.0         # Latest rolling mean value
    last_price: float | None = None        # Previous price (for anomaly detection)
    peak_price: float = -math.inf          # Highest observed price (for drawdown)
    max_drawdown_pct: float = 0.0          # Maximum drawdown percentage
    anomaly_count: int = 0                 # Count of detected anomalies

    def update(self, price: float, volume: int) -> None:
        """Update symbol state with a new tick (price, volume)."""
        
        # Update volume and notional for VWAP calculation
        self.total_volume += volume
        self.total_notional += price * volume

        # Maintain rolling window and compute moving average
        self.rolling_prices.append(price)
        self.rolling_mean_last = round(sum(self.rolling_prices) / len(self.rolling_prices), 4)

        # Detect anomalies based on percentage price change
        if self.last_price is not None and self.last_price != 0:
            price_jump_pct = abs((price - self.last_price) / self.last_price) * 100
            if price_jump_pct >= ANOMALY_THRESHOLD_PCT:
                self.anomaly_count += 1

        # Track peak price for drawdown calculation
        self.peak_price = max(self.peak_price, price)

        # Compute drawdown (drop from peak)
        if self.peak_price > 0:
            drawdown_pct = ((self.peak_price - price) / self.peak_price) * 100
            self.max_drawdown_pct = max(self.max_drawdown_pct, drawdown_pct)

        # Store current price for next iteration
        self.last_price = price

    def to_dict(self) -> Dict[str, float | int]:
        """Convert internal state to a serialisable dictionary."""
        
        vwap = round(self.total_notional / self.total_volume, 4) if self.total_volume else 0.0

        return {
            "total_volume": self.total_volume,
            "vwap": round(vwap, 4),
            "rolling_mean_last": round(self.rolling_mean_last, 4),
            "anomaly_count": self.anomaly_count,
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
        }


def parse_rows(csv_path: Path) -> Iterable[dict]:
    """Stream and validate rows from a CSV file.

    Skips malformed rows and ensures correct data types.
    """
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            # Basic validation of required fields
            if not row.get("timestamp") or not row.get("symbol"):
                continue

            # Convert fields to correct types
            try:
                row["price"] = float(row["price"])
                row["volume"] = int(row["volume"])
            except (TypeError, ValueError):
                continue

            yield row


def process_ticks(csv_path: Path) -> Dict[str, Dict[str, float | int]]:
    """Process tick data and compute per-symbol metrics."""
    
    states: Dict[str, SymbolState] = defaultdict(SymbolState)

    # Update state for each row (tick)
    for row in parse_rows(csv_path):
        states[row["symbol"]].update(price=row["price"], volume=row["volume"])

    # Convert results into serialisable output
    return {symbol: state.to_dict() for symbol, state in sorted(states.items())}


def main(argv: List[str]) -> int:
    """Entry point for CLI execution."""
    
    if len(argv) != 3:
        print("Usage: python3 pipeline.py <input_csv> <output_json>")
        return 1

    input_path = Path(argv[1])
    output_path = Path(argv[2])

    # Run processing pipeline
    results = process_ticks(input_path)

    # Write output as formatted JSON
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print(f"Processed {len(results)} symbols. Output written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

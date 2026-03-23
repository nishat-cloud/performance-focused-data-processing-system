from __future__ import annotations

import csv
import json
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, Iterable, List


ROLLING_WINDOW = 3
ANOMALY_THRESHOLD_PCT = 0.8


@dataclass
class SymbolState:
    total_volume: int = 0
    total_notional: float = 0.0
    rolling_prices: Deque[float] = field(default_factory=lambda: deque(maxlen=ROLLING_WINDOW))
    rolling_mean_last: float = 0.0
    last_price: float | None = None
    peak_price: float = -math.inf
    max_drawdown_pct: float = 0.0
    anomaly_count: int = 0

    def update(self, price: float, volume: int) -> None:
        self.total_volume += volume
        self.total_notional += price * volume

        self.rolling_prices.append(price)
        self.rolling_mean_last = round(sum(self.rolling_prices) / len(self.rolling_prices), 4)

        if self.last_price is not None and self.last_price != 0:
            price_jump_pct = abs((price - self.last_price) / self.last_price) * 100
            if price_jump_pct >= ANOMALY_THRESHOLD_PCT:
                self.anomaly_count += 1

        self.peak_price = max(self.peak_price, price)
        if self.peak_price > 0:
            drawdown_pct = ((self.peak_price - price) / self.peak_price) * 100
            self.max_drawdown_pct = max(self.max_drawdown_pct, drawdown_pct)

        self.last_price = price

    def to_dict(self) -> Dict[str, float | int]:
        vwap = round(self.total_notional / self.total_volume, 4) if self.total_volume else 0.0
        return {
            "total_volume": self.total_volume,
            "vwap": round(vwap, 4),
            "rolling_mean_last": round(self.rolling_mean_last, 4),
            "anomaly_count": self.anomaly_count,
            "max_drawdown_pct": round(self.max_drawdown_pct, 4),
        }


def parse_rows(csv_path: Path) -> Iterable[dict]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row.get("timestamp") or not row.get("symbol"):
                continue
            try:
                row["price"] = float(row["price"])
                row["volume"] = int(row["volume"])
            except (TypeError, ValueError):
                continue
            yield row


def process_ticks(csv_path: Path) -> Dict[str, Dict[str, float | int]]:
    states: Dict[str, SymbolState] = defaultdict(SymbolState)
    for row in parse_rows(csv_path):
        states[row["symbol"]].update(price=row["price"], volume=row["volume"])
    return {symbol: state.to_dict() for symbol, state in sorted(states.items())}


def main(argv: List[str]) -> int:
    if len(argv) != 3:
        print("Usage: python3 pipeline.py <input_csv> <output_json>")
        return 1

    input_path = Path(argv[1])
    output_path = Path(argv[2])

    results = process_ticks(input_path)
    output_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Processed {len(results)} symbols. Output written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

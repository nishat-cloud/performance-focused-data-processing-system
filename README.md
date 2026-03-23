# Performance-Focused Data Processing System

A small project that processes market-style tick data and compares two implementations of the same pipeline:

- **Python** version focused on readability and fast iteration
- **C++** version focused on performance and low-overhead processing

The project ingests CSV data, validates records, computes rolling metrics, aggregates per-symbol analytics, detects anomalies, and writes structured outputs. It is designed to demonstrate systems thinking, performance awareness, and clean engineering practices.

## Why I built this

I wanted a project that reflects the kind of engineering I enjoy most: understanding how data moves through a system, reducing waste in processing logic, and building something that is reliable enough to test and reason about.

Although this is a compact project, the same ideas show up in larger production systems:
- efficient parsing and validation
- clear separation between ingestion, processing, and output
- profiling and runtime comparison
- test coverage for correctness
- structured logging and error handling

## Features

- Parses tick-style CSV data with timestamp, symbol, price, and volume
- Validates malformed or incomplete rows
- Computes per-symbol metrics:
  - total traded volume
  - VWAP (volume-weighted average price)
  - rolling moving average
  - max drawdown approximation
  - anomaly counts based on price jumps
- Benchmarks Python and C++ runtimes on the same dataset
- Writes results to JSON for downstream use
- Simulated real-time data streaming to mimic live market data ingestion
  
## Repository Structure

```
performance-focused-data-processing-system/
├── data/
│   └── sample_ticks.csv
├── docs/
│   └── notes.md
├── src/
│   ├── python/
│   │   ├── pipeline.py
│   │   └── benchmark.py
│   └── cpp/
│       └── pipeline.cpp
├── tests/
│   └── test_pipeline.py
├── .gitignore
└── README.md
```

## Tech Stack

- **Python 3** for fast development and analytics logic
- **C++17** for a performance-oriented implementation
- **pytest** for validation

## Example output

```json
{
  "AAPL": {
    "total_volume": 725,
    "vwap": 185.11,
    "rolling_mean_last": 185.15,
    "anomaly_count": 1,
    "max_drawdown_pct": 0.43
  }
}
```

## How to run

### Python pipeline

```bash
cd src/python
python3 pipeline.py ../../data/sample_ticks.csv ../../data/output_python.json
```

### Python benchmark

```bash
cd src/python
python3 benchmark.py ../../data/sample_ticks.csv
```

### C++ pipeline

```bash
cd src/cpp
g++ -O3 -std=c++17 pipeline.cpp -o pipeline
./pipeline ../../data/sample_ticks.csv ../../data/output_cpp.json
```

### Tests

```bash
pytest tests/test_pipeline.py
```

## What this project demonstrates

This project is intentionally simple in scope but strong in signal. It demonstrates:

- comfort working across **Python and C++**
- attention to **performance, validation, and correctness**
- understanding of **data processing pipelines** and system trade-offs
- ability to write code that is structured, testable, and easy to extend

## Improvements I would make next

- stream processing instead of file-based batch processing
- configurable anomaly thresholds by symbol volatility
- memory profiling and larger benchmark dataset generation
- optional REST endpoint for serving computed metrics

## Notes for reviewers

The dataset is synthetic and intentionally small for readability. The main value of the repo is the design approach and the performance-minded implementation choices rather than domain-specific financial complexity.

## Results

In testing, the C++ implementation achieved significantly lower runtime compared to Python, highlighting the trade-offs between development speed and execution performance.

from pathlib import Path
import sys

# Add project source directory to Python path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src" / "python"))

from pipeline import process_ticks  # noqa: E402


def test_process_ticks_returns_expected_symbols():
    """
    Ensure the pipeline correctly processes input data
    and returns all expected symbols.
    """
    input_path = PROJECT_ROOT / "data" / "sample_ticks.csv"
    results = process_ticks(input_path)

    assert set(results.keys()) == {"AAPL", "MSFT", "NVDA"}


def test_total_volume_for_aapl():
    """
    Validate that total traded volume is aggregated correctly
    for a known symbol (AAPL).
    """
    input_path = PROJECT_ROOT / "data" / "sample_ticks.csv"
    results = process_ticks(input_path)

    assert results["AAPL"]["total_volume"] == 725


def test_anomaly_count_detects_large_jump():
    """
    Verify that anomaly detection logic flags at least one
    significant price jump for NVDA.
    """
    input_path = PROJECT_ROOT / "data" / "sample_ticks.csv"
    results = process_ticks(input_path)

    assert results["NVDA"]["anomaly_count"] >= 1

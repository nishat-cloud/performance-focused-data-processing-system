from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from pipeline import process_ticks


def time_python(input_path: Path, runs: int = 2000) -> float:
    """Measure total runtime of the Python pipeline over multiple runs."""
    start = time.perf_counter()
    for _ in range(runs):
        process_ticks(input_path)
    end = time.perf_counter()
    return end - start


def time_cpp(input_path: Path, runs: int = 2000) -> float | None:
    """Measure total runtime of the compiled C++ pipeline over multiple runs.

    Returns None if the C++ binary has not been compiled yet.
    """
    # Locate the compiled C++ binary relative to this script
    cpp_dir = Path(__file__).resolve().parents[1] / "cpp"
    binary = cpp_dir / "pipeline"

    if not binary.exists():
        return None

    start = time.perf_counter()
    for _ in range(runs):
        # Run the C++ executable on the same input file and discard output
        subprocess.run(
            [str(binary), str(input_path), "/tmp/output_cpp.json"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    end = time.perf_counter()
    return end - start


def main() -> int:
    """Benchmark Python and C++ implementations on the same input dataset."""
    if len(sys.argv) != 2:
        print("Usage: python3 benchmark.py <input_csv>")
        return 1

    input_path = Path(sys.argv[1]).resolve()

    # Benchmark Python implementation
    python_time = time_python(input_path)
    print(f"Python runtime: {python_time:.4f}s")

    # Benchmark C++ implementation if compiled binary is available
    cpp_time = time_cpp(input_path)
    if cpp_time is None:
        print("C++ binary not found. Compile src/cpp/pipeline.cpp first.")
    else:
        print(f"C++ runtime:    {cpp_time:.4f}s")

        # Print approximate relative speedup of C++ vs Python
        if cpp_time > 0:
            print(f"Approx speedup: {python_time / cpp_time:.2f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

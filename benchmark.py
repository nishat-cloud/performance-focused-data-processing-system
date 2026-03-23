from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

from pipeline import process_ticks


def time_python(input_path: Path, runs: int = 2000) -> float:
    start = time.perf_counter()
    for _ in range(runs):
        process_ticks(input_path)
    end = time.perf_counter()
    return end - start


def time_cpp(input_path: Path, runs: int = 2000) -> float | None:
    cpp_dir = Path(__file__).resolve().parents[1] / "cpp"
    binary = cpp_dir / "pipeline"
    if not binary.exists():
        return None

    start = time.perf_counter()
    for _ in range(runs):
        subprocess.run(
            [str(binary), str(input_path), "/tmp/output_cpp.json"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    end = time.perf_counter()
    return end - start


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python3 benchmark.py <input_csv>")
        return 1

    input_path = Path(sys.argv[1]).resolve()
    python_time = time_python(input_path)
    print(f"Python runtime: {python_time:.4f}s")

    cpp_time = time_cpp(input_path)
    if cpp_time is None:
        print("C++ binary not found. Compile src/cpp/pipeline.cpp first.")
    else:
        print(f"C++ runtime:    {cpp_time:.4f}s")
        if cpp_time > 0:
            print(f"Approx speedup: {python_time / cpp_time:.2f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import time
import csv


def stream_data(file_path, delay=0.1):
    """
    Simulate streaming data from a CSV file.

    Yields one row at a time with a delay to mimic real-time data ingestion.
    """
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)

        for row in reader:
            yield row  # Emit one "tick" at a time
            time.sleep(delay)  # Simulate latency between incoming data


def process_stream(file_path):
    """
    Process incoming streamed data.

    Currently prints each tick, but could be extended to update
    metrics, trigger alerts, or feed into a live system.
    """
    print("Starting stream processing...\n")

    for tick in stream_data(file_path):
        # Example processing: log incoming tick
        print(f"Processing: {tick['symbol']} @ {tick['price']}")


if __name__ == "__main__":
    # Entry point for running the streaming simulation
    process_stream("../../data/sample_ticks.csv")

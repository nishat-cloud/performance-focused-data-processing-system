import time
import csv

def stream_data(file_path, delay=0.1):
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row
            time.sleep(delay)

def process_stream(file_path):
    print("Starting stream processing...\n")

    for tick in stream_data(file_path):
        print(f"Processing: {tick['symbol']} @ {tick['price']}")

if __name__ == "__main__":
    process_stream("../../data/sample_ticks.csv")

# crawler/main.py
# Main entry point for the Tuchtrecht crawler.

import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import jsonlines
import argparse

# Ensure the package is importable when executed directly as a script.
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler.sru_client import get_records
from crawler.parser import parse_record
from crawler.scrubber import scrub_text

DATA_DIR = "data"
LAST_UPDATE_FILE = ".last_update"
BASE_QUERY = "c.product-area==tuchtrecht"
RECORDS_PER_SHARD = 1000
DEFAULT_MAX_RECORDS = 10000


def get_last_run_date():
    """Reads the last run timestamp from the .last_update file."""
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, "r") as f:
            return f.read().strip()
    return None


def save_last_run_date():
    """Saves the current timestamp to the .last_update file."""
    with open(LAST_UPDATE_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the Tuchtrecht crawler")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore the last update timestamp and crawl the full backlog",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="Maximum number of records to process in a single run",
    )
    return parser.parse_args()


def main() -> None:
    """Main function to run the crawler."""
    args = parse_args()

    if args.reset and os.path.exists(LAST_UPDATE_FILE):
        os.remove(LAST_UPDATE_FILE)

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    last_run_date = None if args.reset else get_last_run_date()

    if last_run_date:
        print(f"Performing weekly update since last run on: {last_run_date}")
    else:
        print("Performing full backlog crawl.")
    print(f"Maximum records this run: {args.max_records}")

    records_iterator = get_records(BASE_QUERY, start_date=last_run_date)

    shard_index = 0
    records_in_current_shard = 0

    # Find the latest shard index to append to it.
    existing_shards = [
        f for f in os.listdir(DATA_DIR) if f.startswith("tuchtrecht_shard_")
    ]
    if existing_shards:
        shard_index = max(
            [int(f.split("_")[-1].split(".")[0]) for f in existing_shards]
        )
        # Check if the latest shard is full
        with jsonlines.open(
            os.path.join(DATA_DIR, f"tuchtrecht_shard_{shard_index:03d}.jsonl")
        ) as reader:
            records_in_current_shard = sum(1 for _ in reader)
        if records_in_current_shard >= RECORDS_PER_SHARD:
            shard_index += 1
            records_in_current_shard = 0

    output_file = os.path.join(DATA_DIR, f"tuchtrecht_shard_{shard_index:03d}.jsonl")
    writer = jsonlines.open(output_file, mode="a")

    processed_count = 0
    for record in records_iterator:
        parsed = parse_record(record)
        if parsed:
            parsed["Content"] = scrub_text(parsed["Content"])

            writer.write(parsed)
            processed_count += 1
            records_in_current_shard += 1
            print(f"Saved record {processed_count}: {parsed['URL']}")

            if processed_count >= args.max_records:
                print(f"Reached max-records limit ({args.max_records}). Stopping early.")
                break

            if records_in_current_shard >= RECORDS_PER_SHARD:
                writer.close()
                shard_index += 1
                records_in_current_shard = 0
                output_file = os.path.join(
                    DATA_DIR, f"tuchtrecht_shard_{shard_index:03d}.jsonl"
                )
                writer = jsonlines.open(output_file, mode="w")

    writer.close()

    print(f"Processed and saved {processed_count} records.")

    if processed_count > 0 or not last_run_date:
        save_last_run_date()


if __name__ == "__main__":
    main()

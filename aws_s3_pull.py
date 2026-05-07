import os
import json
import time
import boto3
import urllib.parse
import pandas as pd

from pathlib import Path
from datetime import datetime, timezone

# ── AWS SETTINGS ───────────────────────────────────────────

AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.environ["AWS_REGION"]
S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]

# Optional root folder inside bucket
S3_PREFIX = os.environ.get("S3_PREFIX", "")

# ── FILE SETTINGS ──────────────────────────────────────────

STATE_FILE     = "aws_pull_state.json"
OUTPUT_PATTERN = "aws_telemetry_{site_id}.csv"

BATCH_SIZE      = 5000
MAX_RUN_SECONDS = 19800

# ── STATE MANAGEMENT ───────────────────────────────────────

def load_state():

    if Path(STATE_FILE).exists():
        with open(STATE_FILE, "r") as f:
            return json.load(f)

    return {
        "processed_keys": []
    }

def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ── AWS CLIENT ─────────────────────────────────────────────

def get_s3_client():

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

# ── PARSE OBJECT KEY ───────────────────────────────────────

def parse_s3_key(key):

    """
    Example key:

    SiteA/%7B%22siteId%22%3A%22A%22...%7D

    OR

    SiteA/{"siteId":"A",...}
    """

    filename = key.split("/")[-1]

    # URL decode if encoded
    decoded = urllib.parse.unquote(filename)

    try:
        data = json.loads(decoded)

        return data

    except Exception as e:

        print(f"Failed parsing key: {key}")
        print(e)

        return None

# ── DATA CLEANING ──────────────────────────────────────────

def flatten_batch(df):

    if "telemetryDate" in df.columns:
        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"],
            errors="coerce",
            utc=True
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    numeric_cols = [
        "trackerCurrentAngle",
        "trackerTargetAngle",
        "motorCurrent",
        "batteryCharge",
        "batteryTemperature",
        "batteryCycleCount",
        "pvVoltage",
        "pvCurrent",
        "irradiance"
    ]

    for col in numeric_cols:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    return df

# ── CSV WRITING ────────────────────────────────────────────

def append_batch_to_csv(batch_df, output_file):

    file_exists = Path(output_file).exists()

    batch_df.to_csv(
        output_file,
        mode="a",
        header=not file_exists,
        index=False
    )

def write_batch_by_site(batch_df):

    site_files = set()

    for site_id, site_df in batch_df.groupby("siteId", dropna=False):

        safe_site = str(site_id).replace("/", "-")
        safe_site = safe_site.replace(" ", "_")

        if safe_site == "nan":
            safe_site = "Unknown"

        output_file = OUTPUT_PATTERN.format(
            site_id=safe_site
        )

        append_batch_to_csv(site_df, output_file)

        site_files.add(safe_site)

    return site_files

# ── DEDUPLICATION ──────────────────────────────────────────

def deduplicate_csv(output_file):

    if not Path(output_file).exists():
        return

    df = pd.read_csv(output_file, dtype=str)

    dedup_cols = [
        "siteId",
        "deviceId",
        "telemetryDate"
    ]

    available = [
        c for c in dedup_cols
        if c in df.columns
    ]

    if available:

        df = df.drop_duplicates(
            subset=available,
            keep="last"
        )

    df.to_csv(output_file, index=False)

# ── MAIN ───────────────────────────────────────────────────

def main():

    print("\n" + "=" * 50)
    print("AWS S3 Pull Started")
    print("=" * 50)

    start_time = time.time()

    state = load_state()

    processed_keys = set(
        state.get("processed_keys", [])
    )

    s3 = get_s3_client()

    paginator = s3.get_paginator("list_objects_v2")

    batch = []
    total_rows = 0
    site_files = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX
    ):

        contents = page.get("Contents", [])

        for obj in contents:

            elapsed = time.time() - start_time

            if elapsed >= MAX_RUN_SECONDS:

                print("Stopping due to runtime limit")

                save_state({
                    "processed_keys": list(processed_keys)
                })

                return

            key = obj["Key"]

            if key.endswith("/"):
                continue

            if key in processed_keys:
                continue

            data = parse_s3_key(key)

            if not data:
                continue

            batch.append(data)

            processed_keys.add(key)

            if len(batch) >= BATCH_SIZE:

                df = pd.DataFrame(batch)

                df = flatten_batch(df)

                written = write_batch_by_site(df)

                site_files.update(written)

                total_rows += len(df)

                batch = []

                save_state({
                    "processed_keys": list(processed_keys)
                })

                print(f"Rows processed: {total_rows:,}")

    # Final partial batch
    if batch:

        df = pd.DataFrame(batch)

        df = flatten_batch(df)

        written = write_batch_by_site(df)

        site_files.update(written)

        total_rows += len(df)

    # Deduplicate
    for site in site_files:

        output_file = OUTPUT_PATTERN.format(
            site_id=site
        )

        deduplicate_csv(output_file)

    save_state({
        "processed_keys": list(processed_keys),
        "last_run": datetime.now(
            timezone.utc
        ).isoformat()
    })

    print("\nCompleted successfully")
    print(f"Total rows processed: {total_rows:,}")

if __name__ == "__main__":
    main()
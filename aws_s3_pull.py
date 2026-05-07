import os
import json
import time
import gzip
import boto3
import pandas as pd

from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone

# ── AWS SETTINGS ───────────────────────────────────────────

AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.environ["AWS_REGION"]
S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]

# Optional S3 folder prefix
S3_PREFIX = os.environ.get("S3_PREFIX", "")

# ── FILE SETTINGS ──────────────────────────────────────────

STATE_FILE     = "aws_pull_state.json"
OUTPUT_PATTERN = "aws_telemetry_{site_id}.csv"

MAX_RUN_SECONDS = 19800  # 5.5 hours

# ── COLUMN NORMALIZATION ───────────────────────────────────

COLUMN_MAP = {
    "Data": "telemetryDate",
    "Position_a1_rad": "trackerCurrentAngle",
    "TargetAngle_a1_rad": "trackerTargetAngle",
    "MotorCurrent_a1_mA": "motorCurrent",
    "PanelVoltage_mV": "pvVoltage",
    "PanelCurrent_mA": "pvCurrent",
    "StateOfCharge": "batteryCharge",
    "TempBat_Kx10": "batteryTemperature",
}

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

# ── CSV PROCESSING ─────────────────────────────────────────

def process_s3_object(s3, key):

    response = s3.get_object(
        Bucket=S3_BUCKET_NAME,
        Key=key
    )

    compressed_data = response["Body"].read()

    with gzip.GzipFile(
        fileobj=BytesIO(compressed_data)
    ) as gz:

        df = pd.read_csv(
            gz,
            sep=";"
        )

    return df

def get_site_id_from_key(key):

    return key.split("/")[0]

# ── DATA CLEANING ──────────────────────────────────────────

def normalize_dataframe(df):

    df = df.rename(columns=COLUMN_MAP)

    if "telemetryDate" in df.columns:

        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"],
            format="%Y%m%d%H%M%S",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    numeric_cols = [
        "trackerCurrentAngle",
        "trackerTargetAngle",
        "motorCurrent",
        "pvVoltage",
        "pvCurrent",
        "batteryCharge",
        "batteryTemperature"
    ]

    for col in numeric_cols:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    return df

# ── CSV WRITING ────────────────────────────────────────────

def append_batch_to_csv(df, output_file):

    file_exists = Path(output_file).exists()

    df.to_csv(
        output_file,
        mode="a",
        header=not file_exists,
        index=False
    )

def deduplicate_csv(output_file):

    if not Path(output_file).exists():
        return

    df = pd.read_csv(output_file, dtype=str)

    dedup_cols = [
        "uniqueKey",
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

    if "telemetryDate" in df.columns:

        df = df.sort_values(
            "telemetryDate"
        )

    df.to_csv(output_file, index=False)

# ── MAIN ───────────────────────────────────────────────────

def main():

    print("\\n" + "=" * 50)
    print("AWS S3 Pull Started")
    print("=" * 50)

    start_time = time.time()

    state = load_state()

    processed_keys = set(
        state.get("processed_keys", [])
    )

    print(f"Previously processed files: {len(processed_keys):,}")

    s3 = get_s3_client()

    paginator = s3.get_paginator("list_objects_v2")

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

            if not key.endswith(".csv.gz"):
                continue

            if key in processed_keys:
                continue

            print(f"Processing: {key}")

            try:

                df = process_s3_object(s3, key)

                site_id = get_site_id_from_key(key)

                df["siteId"] = site_id

                df = normalize_dataframe(df)

                output_file = OUTPUT_PATTERN.format(
                    site_id=site_id
                )

                append_batch_to_csv(
                    df,
                    output_file
                )

                site_files.add(site_id)

                total_rows += len(df)

                processed_keys.add(key)

                save_state({
                    "processed_keys": list(processed_keys)
                })

                print(
                    f"  Added {len(df):,} rows "
                    f"(total: {total_rows:,})"
                )

            except Exception as e:

                print(f"ERROR processing {key}: {e}")

    print("\\nDeduplicating CSV files...")

    for site_id in site_files:

        output_file = OUTPUT_PATTERN.format(
            site_id=site_id
        )

        deduplicate_csv(output_file)

    save_state({
        "processed_keys": list(processed_keys),
        "last_run": datetime.now(
            timezone.utc
        ).isoformat()
    })

    print("\\n" + "=" * 50)
    print("AWS pull completed successfully")
    print(f"Total rows processed: {total_rows:,}")
    print("=" * 50)

if __name__ == "__main__":
    main()

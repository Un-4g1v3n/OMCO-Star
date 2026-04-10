import os
import json
import time
import pandas as pd
from azure.cosmos import CosmosClient
from datetime import datetime, timezone
from pathlib import Path

# ── SETTINGS ───────────────────────────────────────────────
ENDPOINT        = os.environ["COSMOS_ENDPOINT"]
KEY             = os.environ["COSMOS_KEY"]
DATABASE_NAME   = "telemetry"
CONTAINER_NAME  = "device-telemetry"
TIMESTAMP_FILE  = "last_pull_timestamp.json"
OUTPUT_PATTERN  = "telemetry_{site_id}.csv"
BATCH_SIZE      = 5000
MAX_RUN_SECONDS = 19800  # 5.5 hours

# ── SENSOR VALIDITY LIMITS ─────────────────────────────────
# Values outside these ranges are replaced with NaN (bad/sentinel data)
# Adjust thresholds if your hardware has different valid ranges
SENSOR_LIMITS = {
    "irradiance":          (0, 1500),     # W/m² — solar max ~1200, some headroom
    "motorCurrent":        (0, 50),       # Amps
    "pvVoltage":           (0, 1000),     # Volts
    "pvCurrent":           (0, 100),      # Amps
    "batteryCharge":       (0, 100),      # Percent
    "batteryTemperature":  (-50, 100),    # Celsius
    "batteryCycleCount":   (0, 100000),   # Count
    "trackerCurrentAngle": (-180, 180),   # Degrees
    "trackerTargetAngle":  (-180, 180),   # Degrees
}

# ── HELPERS ────────────────────────────────────────────────

def load_state():
    if Path(TIMESTAMP_FILE).exists():
        with open(TIMESTAMP_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_pull"), data.get("checkpoint_date")
    return None, None


def save_state(last_pull=None, checkpoint_date=None):
    existing = {}
    if Path(TIMESTAMP_FILE).exists():
        with open(TIMESTAMP_FILE, "r") as f:
            existing = json.load(f)
    if last_pull is not None:
        existing["last_pull"] = last_pull
    if checkpoint_date is not None:
        existing["checkpoint_date"] = checkpoint_date
    with open(TIMESTAMP_FILE, "w") as f:
        json.dump(existing, f)
    print("State saved: {}".format(existing), flush=True)


def clear_checkpoint():
    if Path(TIMESTAMP_FILE).exists():
        with open(TIMESTAMP_FILE, "r") as f:
            data = json.load(f)
        data.pop("checkpoint_date", None)
        with open(TIMESTAMP_FILE, "w") as f:
            json.dump(data, f)


def build_query(since_date):
    query = """
SELECT
    c.message.siteId,
    c.message.deviceId,
    c.message.siteFriendlyName,
    c.message.event,
    c.message.telemetryDate,
    c.message.ts,
    c.message.origData,
    c.message.trackerCurrentAngle,
    c.message.trackerTargetAngle,
    c.message.motorCurrent,
    c.message.batteryCharge,
    c.message.batteryTemperature,
    c.message.batteryCycleCount,
    c.message.batterySystemHealth,
    c.message.pvVoltage,
    c.message.pvCurrent,
    c.message.irradiance
FROM c
WHERE c.message.event IN ('Tracker telemetry', 'Tracker telemetry v13', 'Weather telemetry')
"""
    if since_date:
        query += " AND c.message.telemetryDate > '{}'".format(since_date)
    return query


def extract_telemetry_date(item):
    """Safely extract telemetryDate from nested or flat CosmosDB response."""
    msg = item.get("message")
    if isinstance(msg, dict):
        date_val = msg.get("telemetryDate")
        if date_val:
            return str(date_val)
    date_val = item.get("telemetryDate")
    if date_val:
        return str(date_val)
    return None


def flatten_batch(df):
    """
    Clean up a batch DataFrame:
    - Remove column name prefixes
    - Unpack origData to extract deviceName
    - Drop internal CosmosDB columns
    - Convert datetime columns to timezone-naive strings
    - Convert numeric columns with type safety
    - Apply sensor validity limits (nulls out sentinel/garbage values)
    - Reorder columns logically
    """
    # Remove 'message.' prefix from column names
    df.columns = [col.replace("message.", "") for col in df.columns]

    # Unpack origData dict to extract human-readable device name
    if "origData" in df.columns:
        df["deviceName"] = df["origData"].apply(
            lambda x: x.get("name") if isinstance(x, dict) else None
        )
        df = df.drop(columns=["origData"])

    # Drop CosmosDB internal columns
    internal_cols = ["_rid", "_self", "_etag", "_attachments", "_ts"]
    df = df.drop(columns=internal_cols, errors="ignore")

    # Convert telemetryDate to timezone-naive datetime string
    if "telemetryDate" in df.columns:
        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"], errors="coerce", utc=True
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert ts to timezone-naive datetime string
    # ts is an ISO 8601 string with timezone e.g. "2024-12-22T16:41:41.497224+00:00"
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(
            df["ts"], errors="coerce", utc=True
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Convert numeric columns — cast to string first to safely handle
    # any mixed-type columns before converting (coerces bad values to NaN)
    numeric_cols = [
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "pvVoltage", "pvCurrent", "irradiance"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.strip(), errors="coerce"
            )

    # Apply sensor validity limits — replace out-of-range values with NaN
    # This catches sentinel values like 42949672.35 (0xFFFFFFFF scaled)
    # which indicate missing/invalid data from the device
    for col, (low, high) in SENSOR_LIMITS.items():
        if col in df.columns:
            invalid_mask = (df[col] < low) | (df[col] > high)
            invalid_count = invalid_mask.sum()
            if invalid_count > 0:
                df.loc[invalid_mask, col] = None
                print("  Nulled {:,} out-of-range {} values (outside [{}, {}])".format(
                    invalid_count, col, low, high), flush=True)

    # Reorder columns logically for readability in Power BI
    preferred_order = [
        "siteId", "siteFriendlyName", "deviceId", "deviceName",
        "event", "telemetryDate", "ts",
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "batterySystemHealth", "pvVoltage", "pvCurrent", "irradiance"
    ]
    existing = [c for c in preferred_order if c in df.columns]
    extras   = [c for c in df.columns if c not in preferred_order]
    return df[existing + extras]


def append_batch_to_csv(batch_df, output_file):
    """
    Append a batch to the site CSV file.
    First write includes header. Subsequent writes append without header.
    """
    file_exists = Path(output_file).exists()
    batch_df.to_csv(
        output_file,
        mode="a",
        header=not file_exists,
        index=False
    )
    return sum(1 for _ in open(output_file)) - 1  # row count minus header


def deduplicate_csv(output_file):
    """
    Read full CSV, deduplicate on siteId+deviceId+telemetryDate, sort, rewrite.
    Only called at end of a complete run to keep incremental writes fast.
    """
    if not Path(output_file).exists():
        return 0
    print("  Deduplicating {}...".format(output_file), flush=True)
    df     = pd.read_csv(output_file, dtype=str)
    before = len(df)
    dedup_cols = ["siteId", "deviceId", "telemetryDate"]
    available  = [c for c in dedup_cols if c in df.columns]
    if available:
        df = df.drop_duplicates(subset=available, keep="last")
    sort_cols = [c for c in ["siteId", "deviceId", "telemetryDate"]
                 if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    df.to_csv(output_file, index=False)
    removed = before - len(df)
    print("  {} — {:,} rows ({} duplicates removed)".format(
        output_file, len(df), removed), flush=True)
    return len(df)


def write_batch_by_site(batch_df):
    """Split batch by siteId and append each slice to the correct CSV."""
    results = {}
    for site_id, site_df in batch_df.groupby("siteId", dropna=False):
        safe_site = str(site_id).strip().replace("/", "-").replace(" ", "_")
        if not safe_site or safe_site == "nan":
            safe_site = "Unknown"
        output_file = OUTPUT_PATTERN.format(site_id=safe_site)
        count = append_batch_to_csv(site_df.copy(), output_file)
        results[safe_site] = count
    return results


# ── MAIN ───────────────────────────────────────────────────

def main():
    pull_started_at = datetime.now(timezone.utc).isoformat()
    run_start_time  = time.time()

    print("\n" + "="*50)
    print("Pull started: {}".format(pull_started_at))
    print("Max run time: {:.0f} minutes".format(MAX_RUN_SECONDS / 60))
    print("Output format: CSV per site (no row limit)")
    print("="*50)

    last_timestamp, checkpoint_date = load_state()

    if checkpoint_date:
        since_date = checkpoint_date
        print("Resuming from checkpoint: {}".format(checkpoint_date))
    elif last_timestamp:
        since_date = last_timestamp
        print("Fetching records newer than: {}".format(last_timestamp))
    else:
        since_date = None
        print("First run — fetching ALL data.")

    print("\nConnecting to Cosmos DB...")
    client    = CosmosClient(ENDPOINT, KEY)
    container = (
        client
        .get_database_client(DATABASE_NAME)
        .get_container_client(CONTAINER_NAME)
    )

    query = build_query(since_date)
    print("\nQuery:\n{}".format(query))
    print("\nFetching in batches of {:,}...\n".format(BATCH_SIZE))

    batch          = []
    total_fetched  = 0
    batch_count    = 0
    last_date_seen = None
    timed_out      = False
    site_files     = set()

    for item in container.query_items(
        query=query,
        enable_cross_partition_query=True
    ):
        item_date = extract_telemetry_date(item)
        if item_date:
            last_date_seen = item_date

        batch.append(item)

        elapsed = time.time() - run_start_time
        if elapsed >= MAX_RUN_SECONDS:
            print("\nApproaching 5.5hr limit — stopping to save progress.", flush=True)
            timed_out = True
            break

        if len(batch) >= BATCH_SIZE:
            batch_count   += 1
            batch_df       = pd.DataFrame(batch)
            batch_df       = flatten_batch(batch_df)
            site_counts    = write_batch_by_site(batch_df)
            site_files.update(site_counts.keys())
            total_fetched += len(batch)
            batch          = []

            if last_date_seen:
                save_state(checkpoint_date=last_date_seen)
            else:
                print("WARNING: telemetryDate not found in batch {}".format(
                    batch_count), flush=True)

            print("Batch {:>3} | {:>8,} fetched | {:.1f} min | checkpoint: {} | files: {}".format(
                batch_count, total_fetched,
                elapsed / 60, last_date_seen,
                {k: "{:,}".format(v) for k, v in site_counts.items()}
            ), flush=True)

    # Write final partial batch
    if batch:
        batch_count   += 1
        batch_df       = pd.DataFrame(batch)
        batch_df       = flatten_batch(batch_df)
        site_counts    = write_batch_by_site(batch_df)
        site_files.update(site_counts.keys())
        total_fetched += len(batch)
        elapsed        = time.time() - run_start_time
        if last_date_seen:
            save_state(checkpoint_date=last_date_seen)
        print("Batch {:>3} | {:>8,} fetched | {:.1f} min | checkpoint: {} | files: {}".format(
            batch_count, total_fetched,
            elapsed / 60, last_date_seen,
            {k: "{:,}".format(v) for k, v in site_counts.items()}
        ), flush=True)

    # Final state save
    if timed_out:
        if last_date_seen:
            save_state(checkpoint_date=last_date_seen)
        print("\nTimed out after {:.1f} min. Checkpoint saved at: {}".format(
            (time.time() - run_start_time) / 60, last_date_seen))
        print("Next run will resume from this point.")
    else:
        print("\nDeduplicating and sorting all site files...")
        for site_id in site_files:
            deduplicate_csv(OUTPUT_PATTERN.format(site_id=site_id))
        save_state(last_pull=pull_started_at)
        clear_checkpoint()
        print("\nFull run complete!")

    print("="*50)
    print("Total rows fetched this run: {:,}".format(total_fetched))
    print("="*50)


if __name__ == "__main__":
    main()

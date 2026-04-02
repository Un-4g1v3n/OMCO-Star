import os
import json
import time
import pandas as pd
from azure.cosmos import CosmosClient
from datetime import datetime, timezone
from pathlib import Path

# ── SETTINGS ───────────────────────────────────────────────
ENDPOINT       = os.environ["COSMOS_ENDPOINT"]
KEY            = os.environ["COSMOS_KEY"]
DATABASE_NAME  = "telemetry"
CONTAINER_NAME = "device-telemetry"
OUTPUT_FILE    = "cosmos_telemetry.xlsx"
TIMESTAMP_FILE = "last_pull_timestamp.json"

# Rows per batch before writing to Excel and checkpointing
BATCH_SIZE     = 5000

# Stop fetching after this many seconds and upload what we have.
# GitHub Actions kills jobs at 6 hours (21600s). We stop at 5.5 hours (19800s)
# to leave time for the final Excel write and artifact upload.
MAX_RUN_SECONDS = 19800

# ── HELPERS ────────────────────────────────────────────────

def load_state():
    """Load last pull timestamp and any in-progress checkpoint."""
    if Path(TIMESTAMP_FILE).exists():
        with open(TIMESTAMP_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_pull"), data.get("checkpoint_date")
    return None, None


def save_state(last_pull=None, checkpoint_date=None):
    """Save pull state — preserves existing keys if not overwriting."""
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
    """Clear the checkpoint once a full run completes."""
    if Path(TIMESTAMP_FILE).exists():
        with open(TIMESTAMP_FILE, "r") as f:
            data = json.load(f)
        data.pop("checkpoint_date", None)
        with open(TIMESTAMP_FILE, "w") as f:
            json.dump(data, f)


def build_query(since_date):
    """Build query filtering to needed event types and records newer than since_date."""
    query = """
SELECT
    c.message.siteId,
    c.message.deviceId,
    c.message.event,
    c.message.telemetryDate,
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
    """
    Safely extract telemetryDate from a CosmosDB item.
    Handles both nested (item['message']['telemetryDate'])
    and flattened (item['telemetryDate']) structures.
    """
    # Try nested structure first
    msg = item.get("message")
    if isinstance(msg, dict):
        date_val = msg.get("telemetryDate")
        if date_val:
            return str(date_val)

    # Try flat structure (happens when SELECT aliases flatten the response)
    date_val = item.get("telemetryDate")
    if date_val:
        return str(date_val)

    return None


def flatten_batch(df):
    """Clean up a batch DataFrame — unpack origData, fix types, reorder."""
    # Remove 'message.' prefix from column names
    df.columns = [col.replace("message.", "") for col in df.columns]

    # Unpack origData to extract device name
    if "origData" in df.columns:
        df["deviceName"] = df["origData"].apply(
            lambda x: x.get("name") if isinstance(x, dict) else None
        )
        df = df.drop(columns=["origData"])

    # Drop CosmosDB internal columns
    internal_cols = ["_rid", "_self", "_etag", "_attachments", "_ts"]
    df = df.drop(columns=internal_cols, errors="ignore")

    # Convert telemetryDate to timezone-naive datetime (Excel requirement)
    if "telemetryDate" in df.columns:
        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"], errors="coerce", utc=True
        ).dt.tz_localize(None)

    # Numeric columns
    numeric_cols = [
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "pvVoltage", "pvCurrent", "irradiance"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Reorder columns logically
    preferred_order = [
        "siteId", "deviceId", "deviceName", "event", "telemetryDate",
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "batterySystemHealth", "pvVoltage", "pvCurrent", "irradiance"
    ]
    existing = [c for c in preferred_order if c in df.columns]
    extras   = [c for c in df.columns if c not in preferred_order]
    df = df[existing + extras]

    return df


def append_batch_to_excel(batch_df, output_file):
    """
    Append a batch to the Excel file.
    Deduplicates on siteId + deviceId + telemetryDate and sorts before saving.
    """
    if Path(output_file).exists():
        existing_df = pd.read_excel(output_file)
        if "telemetryDate" in existing_df.columns:
            existing_df["telemetryDate"] = pd.to_datetime(
                existing_df["telemetryDate"], errors="coerce"
            ).dt.tz_localize(None)
        combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
    else:
        combined_df = batch_df

    # Deduplicate
    dedup_cols = ["siteId", "deviceId", "telemetryDate"]
    available  = [c for c in dedup_cols if c in combined_df.columns]
    if available:
        combined_df = combined_df.drop_duplicates(subset=available, keep="last")

    # Sort
    sort_cols = [c for c in ["siteId", "deviceId", "telemetryDate"]
                 if c in combined_df.columns]
    if sort_cols:
        combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)

    combined_df.to_excel(output_file, index=False)
    return len(combined_df)


# ── MAIN ───────────────────────────────────────────────────

def main():
    pull_started_at = datetime.now(timezone.utc).isoformat()
    run_start_time  = time.time()

    print("\n" + "="*50)
    print("Pull started: {}".format(pull_started_at))
    print("Max run time: {:.0f} minutes".format(MAX_RUN_SECONDS / 60))
    print("="*50)

    # Load state
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

    # Connect
    print("\nConnecting to Cosmos DB...")
    client    = CosmosClient(ENDPOINT, KEY)
    container = (
        client
        .get_database_client(DATABASE_NAME)
        .get_container_client(CONTAINER_NAME)
    )

    query = build_query(since_date)
    print("\nQuery:\n{}".format(query))
    print("\nFetching in batches of {:,}...".format(BATCH_SIZE))

    batch           = []
    total_fetched   = 0
    batch_count     = 0
    last_date_seen  = None
    timed_out       = False

    for item in container.query_items(
        query=query,
        enable_cross_partition_query=True
    ):
        # Extract telemetryDate for checkpointing using robust helper
        item_date = extract_telemetry_date(item)
        if item_date:
            last_date_seen = item_date

        batch.append(item)

        # Check if we're approaching the time limit
        elapsed = time.time() - run_start_time
        if elapsed >= MAX_RUN_SECONDS:
            print("\nApproaching 5.5 hour time limit — stopping fetch to save progress.", flush=True)
            timed_out = True
            break

        # Write batch when full
        if len(batch) >= BATCH_SIZE:
            batch_count   += 1
            batch_df       = pd.DataFrame(batch)
            batch_df       = flatten_batch(batch_df)
            total_in_file  = append_batch_to_excel(batch_df, OUTPUT_FILE)
            total_fetched += len(batch)
            batch          = []

            # Save checkpoint after every batch
            if last_date_seen:
                save_state(checkpoint_date=last_date_seen)
            else:
                # If we still can't get a date, save row count as progress marker
                print("WARNING: telemetryDate not found in batch {} — checkpoint not updated.".format(
                    batch_count), flush=True)
                # Print first item structure so we can debug the field path
                print("Sample item keys: {}".format(list(
                    container.query_items(query="SELECT TOP 1 * FROM c", 
                    enable_cross_partition_query=True)).__class__), flush=True)

            elapsed_min = elapsed / 60
            print("Batch {:>3} | {:>8,} fetched | {:>8,} in file | {:.1f} min elapsed | checkpoint: {}".format(
                batch_count, total_fetched, total_in_file,
                elapsed_min, last_date_seen
            ), flush=True)

    # Write final partial batch
    if batch:
        batch_count   += 1
        batch_df       = pd.DataFrame(batch)
        batch_df       = flatten_batch(batch_df)
        total_in_file  = append_batch_to_excel(batch_df, OUTPUT_FILE)
        total_fetched += len(batch)
        elapsed_min    = (time.time() - run_start_time) / 60
        print("Batch {:>3} | {:>8,} fetched | {:>8,} in file | {:.1f} min elapsed | checkpoint: {}".format(
            batch_count, total_fetched, total_in_file,
            elapsed_min, last_date_seen
        ), flush=True)

    # Save final state
    if timed_out:
        # Save checkpoint so next run resumes where we left off
        if last_date_seen:
            save_state(checkpoint_date=last_date_seen)
        print("\nRun timed out after {:.1f} minutes. Checkpoint saved.".format(
            (time.time() - run_start_time) / 60))
        print("Next run will resume from: {}".format(last_date_seen))
    else:
        # Full run completed — save pull timestamp and clear checkpoint
        save_state(last_pull=pull_started_at)
        clear_checkpoint()
        print("\nFull run complete!")

    print("="*50)
    print("Total rows fetched this run: {:,}".format(total_fetched))
    print("="*50)


if __name__ == "__main__":
    main()


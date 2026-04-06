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

# Output file per site — {site_id} is replaced with the actual siteId value
# e.g. "telemetry_SiteA.xlsx", "telemetry_SiteB.xlsx" etc.
OUTPUT_PATTERN  = "telemetry_{site_id}.xlsx"

# Rows per batch before writing to Excel and checkpointing
BATCH_SIZE      = 5000

# Stop fetching at 5.5 hours to allow time for upload before GitHub's 6hr kill
MAX_RUN_SECONDS = 19800

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


def extract_site_id(item):
    """Safely extract siteId from nested or flat CosmosDB response."""
    msg = item.get("message")
    if isinstance(msg, dict):
        site = msg.get("siteId")
        if site:
            return str(site).strip().replace("/", "-").replace(" ", "_")
    site = item.get("siteId")
    if site:
        return str(site).strip().replace("/", "-").replace(" ", "_")
    return "Unknown"


def flatten_batch(df):
    """Clean up column names, unpack origData, enforce types, reorder."""
    df.columns = [col.replace("message.", "") for col in df.columns]

    if "origData" in df.columns:
        df["deviceName"] = df["origData"].apply(
            lambda x: x.get("name") if isinstance(x, dict) else None
        )
        df = df.drop(columns=["origData"])

    internal_cols = ["_rid", "_self", "_etag", "_attachments", "_ts"]
    df = df.drop(columns=internal_cols, errors="ignore")

    if "telemetryDate" in df.columns:
        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"], errors="coerce", utc=True
        ).dt.tz_localize(None)

    numeric_cols = [
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "pvVoltage", "pvCurrent", "irradiance"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    preferred_order = [
        "siteId", "deviceId", "deviceName", "event", "telemetryDate",
        "trackerCurrentAngle", "trackerTargetAngle", "motorCurrent",
        "batteryCharge", "batteryTemperature", "batteryCycleCount",
        "batterySystemHealth", "pvVoltage", "pvCurrent", "irradiance"
    ]
    existing = [c for c in preferred_order if c in df.columns]
    extras   = [c for c in df.columns if c not in preferred_order]
    return df[existing + extras]


def append_batch_to_excel(batch_df, output_file):
    """
    Append a batch to the correct site Excel file.
    Deduplicates and sorts before saving.
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

    dedup_cols = ["siteId", "deviceId", "telemetryDate"]
    available  = [c for c in dedup_cols if c in combined_df.columns]
    if available:
        combined_df = combined_df.drop_duplicates(subset=available, keep="last")

    sort_cols = [c for c in ["siteId", "deviceId", "telemetryDate"]
                 if c in combined_df.columns]
    if sort_cols:
        combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)

    combined_df.to_excel(output_file, index=False)
    return len(combined_df)


def write_batch_by_site(batch_df):
    """
    Split a batch by siteId and append each slice to the correct site file.
    Returns a dict of {site_id: row_count_in_file}.
    """
    results = {}
    for site_id, site_df in batch_df.groupby("siteId", dropna=False):
        # Sanitize site_id for use in filename
        safe_site = str(site_id).strip().replace("/", "-").replace(" ", "_")
        if not safe_site or safe_site == "nan":
            safe_site = "Unknown"
        output_file = OUTPUT_PATTERN.format(site_id=safe_site)
        count = append_batch_to_excel(site_df.copy(), output_file)
        results[safe_site] = count
    return results


# ── MAIN ───────────────────────────────────────────────────

def main():
    pull_started_at = datetime.now(timezone.utc).isoformat()
    run_start_time  = time.time()

    print("\n" + "="*50)
    print("Pull started: {}".format(pull_started_at))
    print("Max run time: {:.0f} minutes".format(MAX_RUN_SECONDS / 60))
    print("Output: one Excel file per site ({})".format(OUTPUT_PATTERN))
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

    for item in container.query_items(
        query=query,
        enable_cross_partition_query=True
    ):
        item_date = extract_telemetry_date(item)
        if item_date:
            last_date_seen = item_date

        batch.append(item)

        # Check time limit
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
        total_fetched += len(batch)
        elapsed        = time.time() - run_start_time
        if last_date_seen:
            save_state(checkpoint_date=last_date_seen)
        print("Batch {:>3} | {:>8,} fetched | {:.1f} min | checkpoint: {} | files: {}".format(
            batch_count, total_fetched,
            elapsed / 60, last_date_seen,
            {k: "{:,}".format(v) for k, v in site_counts.items()}
        ), flush=True)

    # Save final state
    if timed_out:
        if last_date_seen:
            save_state(checkpoint_date=last_date_seen)
        print("\nTimed out after {:.1f} min. Checkpoint saved at: {}".format(
            (time.time() - run_start_time) / 60, last_date_seen))
        print("Next run will resume from this point.")
    else:
        save_state(last_pull=pull_started_at)
        clear_checkpoint()
        print("\nFull run complete!")

    print("="*50)
    print("Total rows fetched this run: {:,}".format(total_fetched))
    print("="*50)


if __name__ == "__main__":
    main()



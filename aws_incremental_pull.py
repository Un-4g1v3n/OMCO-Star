import os
import re
import json
import time
import gzip
import boto3
import requests
import pandas as pd

from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ==========================================================
# AWS SETTINGS
# ==========================================================

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

# ==========================================================
# AZURE / ONEDRIVE SETTINGS
# ==========================================================

CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
TENANT_ID = os.environ["AZURE_TENANT_ID"]
ONEDRIVE_USER_EMAIL = os.environ["ONEDRIVE_USER_EMAIL"]

ONEDRIVE_FOLDER = "/Reliability/AWSDB"

# ==========================================================
# FILE SETTINGS
# ==========================================================

STATE_FILE = "aws_site_state.json"

RETENTION_DAYS = 730
MAX_RUN_SECONDS = 19800

CHECKPOINT_INTERVAL = 100

# 200MB max per CSV part
MAX_CSV_SIZE_BYTES = 200 * 1024 * 1024

# ==========================================================
# COLUMN MAP
# ==========================================================

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

# ==========================================================
# TOKEN
# ==========================================================

def get_graph_token():

    url = (
        f"https://login.microsoftonline.com/"
        f"{TENANT_ID}/oauth2/v2.0/token"
    )

    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }

    response = requests.post(
        url,
        data=payload,
        timeout=30
    )

    response.raise_for_status()

    return response.json()["access_token"]

# ==========================================================
# STATE
# ==========================================================

def load_state():

    if Path(STATE_FILE).exists():

        with open(STATE_FILE, "r") as f:
            return json.load(f)

    return {"sites": {}}

def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ==========================================================
# AWS CLIENT
# ==========================================================

def get_s3_client():

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

# ==========================================================
# TIMESTAMP
# ==========================================================

def extract_timestamp_from_key(key):

    match = re.search(
        r"(\d{4})_(\d{2})_(\d{2})_(\d{6})",
        key
    )

    if not match:
        return None

    year, month, day, hhmmss = match.groups()

    return datetime(
        int(year),
        int(month),
        int(day),
        int(hhmmss[0:2]),
        int(hhmmss[2:4]),
        int(hhmmss[4:6]),
        tzinfo=timezone.utc
    )

# ==========================================================
# CSV PROCESSING
# ==========================================================

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

def normalize_dataframe(df):

    df = df.rename(columns=COLUMN_MAP)

    if "telemetryDate" in df.columns:

        df["telemetryDate"] = pd.to_datetime(
            df["telemetryDate"],
            format="%Y%m%d%H%M%S",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d %H:%M:%S")

    return df

# ==========================================================
# FILE ROTATION
# ==========================================================

def get_active_csv(site_id):

    index = 1

    while True:

        filename = (
            f"aws_telemetry_{site_id}_F{index}.csv"
        )

        path = Path(filename)

        if not path.exists():
            return filename

        if path.stat().st_size < MAX_CSV_SIZE_BYTES:
            return filename

        index += 1

# ==========================================================
# ONEDRIVE DOWNLOAD
# ==========================================================

def download_existing_csvs(token, site_id):

    for i in range(1, 100):

        filename = (
            f"aws_telemetry_{site_id}_F{i}.csv"
        )

        remote_path = (
            f"{ONEDRIVE_FOLDER}/{filename}"
        )

        url = (
            f"https://graph.microsoft.com/v1.0/"
            f"users/{ONEDRIVE_USER_EMAIL}"
            f"/drive/root:{remote_path}:/content"
        )

        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(
            url,
            headers=headers,
            timeout=120
        )

        if response.status_code != 200:
            break

        with open(filename, "wb") as f:
            f.write(response.content)

        print(
            f"Downloaded {filename}",
            flush=True
        )

# ==========================================================
# ONEDRIVE UPLOAD
# ==========================================================

def upload_csv(token, local_path):

    filename = Path(local_path).name

    remote_path = (
        f"{ONEDRIVE_FOLDER}/{filename}"
    )

    upload_url = (
        f"https://graph.microsoft.com/v1.0/"
        f"users/{ONEDRIVE_USER_EMAIL}"
        f"/drive/root:{remote_path}:/content"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/csv"
    }

    with open(local_path, "rb") as f:

        response = requests.put(
            upload_url,
            headers=headers,
            data=f,
            timeout=600
        )

    response.raise_for_status()

    print(
        f"Uploaded {filename}",
        flush=True
    )

# ==========================================================
# SAFE CHECKPOINT
# ==========================================================

def checkpoint_upload(
    token,
    updated_sites,
    state,
    pending_state_updates
):

    print(
        "\n=== CHECKPOINT UPLOAD START ===",
        flush=True
    )

    successful_sites = set()

    for site_id in updated_sites:

        try:

            uploaded_any = False

            for i in range(1, 100):

                filename = (
                    f"aws_telemetry_{site_id}_F{i}.csv"
                )

                if not Path(filename).exists():
                    break

                upload_csv(
                    token,
                    filename
                )

                uploaded_any = True

            if uploaded_any:
                successful_sites.add(site_id)

        except Exception as e:

            print(
                f"ERROR uploading site {site_id}: {e}",
                flush=True
            )

    # ======================================================
    # ONLY ADVANCE STATE AFTER SUCCESSFUL UPLOAD
    # ======================================================

    for site_id in successful_sites:

        if site_id in pending_state_updates:

            state["sites"][site_id] = (
                pending_state_updates[site_id]
            )

    save_state(state)

    print(
        "State file saved",
        flush=True
    )

    print(
        "=== CHECKPOINT COMPLETE ===\n",
        flush=True
    )

# ==========================================================
# SITE PREFIXES
# ==========================================================

def get_site_prefixes(s3):

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    prefixes = []

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Delimiter="/"
    ):

        for prefix in page.get(
            "CommonPrefixes",
            []
        ):

            prefixes.append(
                prefix["Prefix"]
            )

    return prefixes

# ==========================================================
# MAIN
# ==========================================================

def main():

    print("\n" + "=" * 50, flush=True)
    print("AWS Incremental Telemetry Pull", flush=True)
    print("=" * 50, flush=True)

    start_time = time.time()

    cutoff_date = (
        datetime.now(timezone.utc)
        - timedelta(days=RETENTION_DAYS)
    )

    print(
        f"Retention cutoff: {cutoff_date}",
        flush=True
    )

    state = load_state()

    pending_state_updates = {}

    s3 = get_s3_client()

    total_rows = 0
    processed_files = 0
    updated_sites = set()

    print(
        "Loading site prefixes...",
        flush=True
    )

    site_prefixes = get_site_prefixes(s3)

    print(
        f"Found {len(site_prefixes)} site prefixes",
        flush=True
    )

    for site_prefix in site_prefixes:

        elapsed = time.time() - start_time

        if elapsed >= MAX_RUN_SECONDS:

            print(
                "Stopping due to runtime limit",
                flush=True
            )

            token = get_graph_token()

            checkpoint_upload(
                token,
                updated_sites,
                state,
                pending_state_updates
            )

            return

        site_id = site_prefix.rstrip("/")

        print(
            f"\nProcessing site: {site_id}",
            flush=True
        )

        token = get_graph_token()

        download_existing_csvs(
            token,
            site_id
        )

        last_processed_str = (
            state["sites"].get(site_id)
        )

        if last_processed_str:

            last_processed = (
                datetime.fromisoformat(
                    last_processed_str
                )
            )

        else:

            last_processed = cutoff_date

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        file_counter = 0

        for page in paginator.paginate(
            Bucket=S3_BUCKET_NAME,
            Prefix=site_prefix
        ):

            contents = page.get(
                "Contents",
                []
            )

            for obj in contents:

                elapsed = (
                    time.time() - start_time
                )

                if elapsed >= MAX_RUN_SECONDS:

                    print(
                        "Stopping due to runtime limit",
                        flush=True
                    )

                    token = get_graph_token()

                    checkpoint_upload(
                        token,
                        updated_sites,
                        state,
                        pending_state_updates
                    )

                    return

                key = obj["Key"]

                if not key.endswith(".csv.gz"):
                    continue

                file_timestamp = (
                    extract_timestamp_from_key(key)
                )

                if not file_timestamp:
                    continue

                # ==========================================
                # 2 YEAR RETENTION
                # ==========================================

                if file_timestamp < cutoff_date:
                    continue

                # ==========================================
                # INCREMENTAL FILTER
                # ==========================================

                if file_timestamp <= last_processed:
                    continue

                file_counter += 1

                print(
                    f"Processing: {key}",
                    flush=True
                )

                try:

                    df = process_s3_object(
                        s3,
                        key
                    )

                    df["siteId"] = site_id

                    df = normalize_dataframe(df)

                    active_csv = get_active_csv(
                        site_id
                    )

                    file_exists = (
                        Path(active_csv).exists()
                    )

                    df.to_csv(
                        active_csv,
                        mode="a",
                        header=not file_exists,
                        index=False
                    )

                    total_rows += len(df)
                    processed_files += 1

                    updated_sites.add(site_id)

                    pending_state_updates[
                        site_id
                    ] = file_timestamp.isoformat()

                    print(
                        f"  Added {len(df):,} rows "
                        f"to {active_csv}",
                        flush=True
                    )

                    # ======================================
                    # PERIODIC CHECKPOINT
                    # ======================================

                    if (
                        processed_files %
                        CHECKPOINT_INTERVAL
                    ) == 0:

                        token = get_graph_token()

                        checkpoint_upload(
                            token,
                            updated_sites,
                            state,
                            pending_state_updates
                        )

                except Exception as e:

                    print(
                        f"ERROR processing {key}: {e}",
                        flush=True
                    )

        print(
            f"Processed {file_counter} files for site",
            flush=True
        )

    # ======================================================
    # FINAL CHECKPOINT
    # ======================================================

    token = get_graph_token()

    checkpoint_upload(
        token,
        updated_sites,
        state,
        pending_state_updates
    )

    print("\n" + "=" * 50, flush=True)
    print("Run complete", flush=True)

    print(
        f"Files processed: {processed_files:,}",
        flush=True
    )

    print(
        f"Rows added: {total_rows:,}",
        flush=True
    )

    print(
        f"Sites updated: {len(updated_sites):,}",
        flush=True
    )

    print("=" * 50, flush=True)

if __name__ == "__main__":
    main()

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

S3_PREFIX = os.environ.get("S3_PREFIX", "")

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

# Upload/save checkpoint every N processed files
CHECKPOINT_INTERVAL = 100

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
# MICROSOFT GRAPH AUTH
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

    response = requests.post(url, data=payload)

    response.raise_for_status()

    return response.json()["access_token"]

# ==========================================================
# STATE MANAGEMENT
# ==========================================================

def load_state():

    if Path(STATE_FILE).exists():

        with open(STATE_FILE, "r") as f:
            return json.load(f)

    return {
        "sites": {}
    }

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
# TIMESTAMP PARSING
# ==========================================================

def extract_timestamp_from_key(key):

    match = re.search(
        r"(\\d{4})_(\\d{2})_(\\d{2})_(\\d{6})",
        key
    )

    if not match:
        return None

    year, month, day, hhmmss = match.groups()

    hour = hhmmss[0:2]
    minute = hhmmss[2:4]
    second = hhmmss[4:6]

    return datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second),
        tzinfo=timezone.utc
    )

def get_site_id_from_key(key):

    return key.split("/")[0]

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
# ONEDRIVE DOWNLOAD
# ==========================================================

def download_existing_csv(token, site_id):

    local_file = f"aws_telemetry_{site_id}.csv"

    remote_path = (
        f"{ONEDRIVE_FOLDER}/"
        f"aws_telemetry_{site_id}.csv"
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
        headers=headers
    )

    if response.status_code == 200:

        with open(local_file, "wb") as f:
            f.write(response.content)

        print(f"Downloaded existing CSV for {site_id}")

        return True

    return False

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
            data=f
        )

    response.raise_for_status()

    print(f"Uploaded {filename}")

# ==========================================================
# CHECKPOINT SAVE + UPLOAD
# ==========================================================

def checkpoint_upload(token, updated_sites, state):

    print("\\n=== CHECKPOINT UPLOAD START ===")

    for site_id in updated_sites:

        local_csv = f"aws_telemetry_{site_id}.csv"

        if Path(local_csv).exists():

            try:

                upload_csv(
                    token,
                    local_csv
                )

            except Exception as e:

                print(
                    f"ERROR uploading {local_csv}: {e}"
                )

    save_state(state)

    print("State file saved")

    print("=== CHECKPOINT COMPLETE ===\\n")

# ==========================================================
# MAIN
# ==========================================================

def main():

    print("\\n" + "=" * 50)
    print("AWS Incremental Telemetry Pull")
    print("=" * 50)

    start_time = time.time()

    cutoff_date = (
        datetime.now(timezone.utc)
        - timedelta(days=RETENTION_DAYS)
    )

    print(f"Retention cutoff: {cutoff_date}")

    state = load_state()

    token = get_graph_token()

    s3 = get_s3_client()

    paginator = s3.get_paginator("list_objects_v2")

    total_rows = 0
    processed_files = 0
    updated_sites = set()

    for page in paginator.paginate(
        Bucket=S3_BUCKET_NAME,
        Prefix=S3_PREFIX
    ):

        contents = page.get("Contents", [])

        for obj in contents:

            elapsed = time.time() - start_time

            if elapsed >= MAX_RUN_SECONDS:

                print("\\nStopping due to runtime limit")

                checkpoint_upload(
                    token,
                    updated_sites,
                    state
                )

                return

            key = obj["Key"]

            if not key.endswith(".csv.gz"):
                continue

            site_id = get_site_id_from_key(key)

            file_timestamp = extract_timestamp_from_key(key)

            if not file_timestamp:
                continue

            if file_timestamp < cutoff_date:
                continue

            last_processed_str = state["sites"].get(site_id)

            if last_processed_str:

                last_processed = datetime.fromisoformat(
                    last_processed_str
                )

                if file_timestamp <= last_processed:
                    continue

            local_csv = f"aws_telemetry_{site_id}.csv"

            if (
                site_id not in updated_sites and
                not Path(local_csv).exists()
            ):

                download_existing_csv(
                    token,
                    site_id
                )

            print(f"Processing: {key}")

            try:

                df = process_s3_object(
                    s3,
                    key
                )

                df["siteId"] = site_id

                df = normalize_dataframe(df)

                file_exists = Path(local_csv).exists()

                df.to_csv(
                    local_csv,
                    mode="a",
                    header=not file_exists,
                    index=False
                )

                total_rows += len(df)
                processed_files += 1

                updated_sites.add(site_id)

                state["sites"][site_id] = (
                    file_timestamp.isoformat()
                )

                print(
                    f"  Added {len(df):,} rows"
                )

                # ==========================================
                # PERIODIC CHECKPOINT
                # ==========================================

                if (
                    processed_files %
                    CHECKPOINT_INTERVAL
                ) == 0:

                    checkpoint_upload(
                        token,
                        updated_sites,
                        state
                    )

            except Exception as e:

                print(
                    f"ERROR processing {key}: {e}"
                )

    print("\\nFinal upload checkpoint...")

    checkpoint_upload(
        token,
        updated_sites,
        state
    )

    print("\\n" + "=" * 50)
    print("Run complete")
    print(f"Files processed: {processed_files:,}")
    print(f"Rows added: {total_rows:,}")
    print(f"Sites updated: {len(updated_sites):,}")
    print("=" * 50)

if __name__ == "__main__":
    main()

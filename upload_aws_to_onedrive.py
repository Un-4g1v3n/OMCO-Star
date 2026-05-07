"""
Uploads all aws_telemetry_*.csv files to OneDrive via Microsoft Graph API.
"""

import os
import sys
import json
import requests

from pathlib import Path

# ── SETTINGS ───────────────────────────────────────────────

CLIENT_ID           = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET       = os.environ["AZURE_CLIENT_SECRET"]
TENANT_ID           = os.environ["AZURE_TENANT_ID"]
ONEDRIVE_USER_EMAIL = os.environ["ONEDRIVE_USER_EMAIL"]

ONEDRIVE_FOLDER = "/Reliability/AWSDB"

# ── AUTH ───────────────────────────────────────────────────

def get_access_token():

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
        data=payload
    )

    response.raise_for_status()

    return response.json()["access_token"]

# ── FOLDER ─────────────────────────────────────────────────

def ensure_folder(token, folder_path):

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    parts = [
        p for p in folder_path.strip("/").split("/")
        if p
    ]

    current_path = ""

    for part in parts:

        current_path += f"/{part}"

        check_url = (
            f"https://graph.microsoft.com/v1.0/"
            f"users/{ONEDRIVE_USER_EMAIL}"
            f"/drive/root:{current_path}"
        )

        response = requests.get(
            check_url,
            headers=headers
        )

        if response.status_code == 404:

            parent_path = (
                "/".join(current_path.split("/")[:-1])
                or "/"
            )

            if parent_path == "/":

                parent_url = (
                    f"https://graph.microsoft.com/v1.0/"
                    f"users/{ONEDRIVE_USER_EMAIL}"
                    f"/drive/root/children"
                )

            else:

                parent_url = (
                    f"https://graph.microsoft.com/v1.0/"
                    f"users/{ONEDRIVE_USER_EMAIL}"
                    f"/drive/root:{parent_path}:/children"
                )

            payload = {
                "name": part,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "rename"
            }

            create_response = requests.post(
                parent_url,
                headers=headers,
                data=json.dumps(payload)
            )

            create_response.raise_for_status()

# ── UPLOAD ─────────────────────────────────────────────────

def upload_file(token, local_path):

    filename = Path(local_path).name

    remote_path = (
        f"{ONEDRIVE_FOLDER.rstrip('/')}/{filename}"
    )

    headers = {
        "Authorization": f"Bearer {token}"
    }

    file_size = Path(local_path).stat().st_size

    print(
        f"Uploading {filename} "
        f"({file_size / 1024 / 1024:.1f} MB)"
    )

    session_url = (
        f"https://graph.microsoft.com/v1.0/"
        f"users/{ONEDRIVE_USER_EMAIL}"
        f"/drive/root:{remote_path}:/createUploadSession"
    )

    session_payload = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace",
            "name": filename
        }
    }

    session_response = requests.post(
        session_url,
        headers={
            **headers,
            "Content-Type": "application/json"
        },
        data=json.dumps(session_payload)
    )

    session_response.raise_for_status()

    upload_url = session_response.json()["uploadUrl"]

    chunk_size = 10 * 1024 * 1024

    uploaded = 0

    with open(local_path, "rb") as f:

        while True:

            chunk = f.read(chunk_size)

            if not chunk:
                break

            chunk_len = len(chunk)

            end_byte = uploaded + chunk_len - 1

            chunk_headers = {
                "Content-Length": str(chunk_len),
                "Content-Range":
                    f"bytes {uploaded}-{end_byte}/{file_size}"
            }

            chunk_response = requests.put(
                upload_url,
                headers=chunk_headers,
                data=chunk
            )

            if chunk_response.status_code not in (200, 201, 202):

                raise ValueError(
                    f"Upload failed: {chunk_response.text}"
                )

            uploaded += chunk_len

    print(f"✓ {filename} uploaded successfully")

# ── MAIN ───────────────────────────────────────────────────

def main():

    print("\\n" + "=" * 50)
    print("AWS OneDrive Upload Started")
    print("=" * 50)

    csv_files = sorted(
        Path(".").glob("aws_telemetry_*.csv")
    )

    if not csv_files:

        print("No AWS telemetry CSV files found")

        sys.exit(0)

    token = get_access_token()

    ensure_folder(
        token,
        ONEDRIVE_FOLDER
    )

    failed = []

    for csv_path in csv_files:

        try:

            upload_file(
                token,
                str(csv_path)
            )

        except Exception as e:

            print(
                f"ERROR uploading "
                f"{csv_path.name}: {e}"
            )

            failed.append(csv_path.name)

    print("\\n" + "=" * 50)

    if failed:

        print("Upload completed with errors")

        for f in failed:
            print(f" - {f}")

        sys.exit(1)

    else:

        print("All AWS CSVs uploaded successfully")

    print("=" * 50)

if __name__ == "__main__":
    main()

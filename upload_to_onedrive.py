"""
Uploads all telemetry_*.csv files to OneDrive via Microsoft Graph API.
Called by GitHub Actions after the main cosmos_pull.py script completes.
"""
import os
import sys
import json
import requests
from pathlib import Path

# ── SETTINGS (all from GitHub secrets) ─────────────────────
CLIENT_ID     = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
TENANT_ID     = os.environ["AZURE_TENANT_ID"]

# OneDrive folder path where CSVs will be uploaded
# Update this to match your exact OneDrive folder
ONEDRIVE_FOLDER = "/Reliability/CosmosDB"

# ── AUTH ───────────────────────────────────────────────────

def get_access_token():
    """Get an OAuth2 access token from Microsoft identity platform."""
    url = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(TENANT_ID)
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://graph.microsoft.com/.default"
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("No access token returned: {}".format(response.json()))
    print("Successfully authenticated with Microsoft Graph.", flush=True)
    return token


def get_user_id(token):
    """
    Get the OneDrive user ID to upload to.
    Uses the first user found — update ONEDRIVE_USER_EMAIL in secrets
    to target a specific user if needed.
    """
    user_email = os.environ.get("ONEDRIVE_USER_EMAIL")
    headers = {"Authorization": "Bearer {}".format(token)}

    if user_email:
        url = "https://graph.microsoft.com/v1.0/users/{}".format(user_email)
    else:
        # Fall back to listing users and picking the first
        url = "https://graph.microsoft.com/v1.0/users"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    if user_email:
        user_id = data.get("id")
    else:
        users = data.get("value", [])
        if not users:
            raise ValueError("No users found in tenant.")
        user_id = users[0]["id"]
        print("Targeting OneDrive of: {}".format(users[0].get("userPrincipalName")))

    return user_id


# ── UPLOAD ─────────────────────────────────────────────────

def ensure_folder(token, user_id, folder_path):
    """
    Ensure the target OneDrive folder exists, creating it if needed.
    folder_path should be like '/Reliability/CosmosDB'
    """
    headers = {
        "Authorization": "Bearer {}".format(token),
        "Content-Type":  "application/json"
    }

    # Build folder path step by step
    parts = [p for p in folder_path.strip("/").split("/") if p]
    current_path = ""

    for part in parts:
        current_path += "/{}".format(part)
        check_url = (
            "https://graph.microsoft.com/v1.0/users/{}/drive/root:{}"
            .format(user_id, current_path)
        )
        response = requests.get(check_url, headers=headers)

        if response.status_code == 404:
            # Folder doesn't exist — create it
            parent_path = "/".join(current_path.split("/")[:-1]) or "/"
            if parent_path == "/":
                parent_url = (
                    "https://graph.microsoft.com/v1.0/users/{}/drive/root/children"
                    .format(user_id)
                )
            else:
                parent_url = (
                    "https://graph.microsoft.com/v1.0/users/{}/drive/root:{}:/children"
                    .format(user_id, parent_path)
                )
            create_payload = {
                "name":   part,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "rename"
            }
            create_response = requests.post(
                parent_url, headers=headers,
                data=json.dumps(create_payload)
            )
            create_response.raise_for_status()
            print("Created folder: {}".format(current_path), flush=True)
        elif response.status_code == 200:
            print("Folder exists: {}".format(current_path), flush=True)
        else:
            response.raise_for_status()


def upload_file(token, user_id, local_path, onedrive_folder):
    """
    Upload a single file to OneDrive using the Graph API upload session.
    Handles files of any size using chunked upload.
    """
    filename    = Path(local_path).name
    remote_path = "{}/{}".format(onedrive_folder.rstrip("/"), filename)
    headers     = {"Authorization": "Bearer {}".format(token)}

    file_size = Path(local_path).stat().st_size
    print("Uploading {} ({:.1f} MB)...".format(
        filename, file_size / 1024 / 1024), flush=True)

    # Create upload session for large files
    session_url = (
        "https://graph.microsoft.com/v1.0/users/{}/drive/root:{}:/createUploadSession"
        .format(user_id, remote_path)
    )
    session_payload = {
        "item": {
            "@microsoft.graph.conflictBehavior": "replace",
            "name": filename
        }
    }
    session_response = requests.post(
        session_url,
        headers={**headers, "Content-Type": "application/json"},
        data=json.dumps(session_payload)
    )
    session_response.raise_for_status()
    upload_url = session_response.json()["uploadUrl"]

    # Upload in 10MB chunks
    chunk_size  = 10 * 1024 * 1024
    uploaded    = 0

    with open(local_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk_len = len(chunk)
            end_byte  = uploaded + chunk_len - 1
            chunk_headers = {
                "Content-Length": str(chunk_len),
                "Content-Range":  "bytes {}-{}/{}".format(
                    uploaded, end_byte, file_size)
            }
            chunk_response = requests.put(
                upload_url, headers=chunk_headers, data=chunk
            )
            if chunk_response.status_code not in (200, 201, 202):
                raise ValueError("Upload failed: {}".format(chunk_response.text))
            uploaded += chunk_len
            pct = (uploaded / file_size) * 100
            print("  {:.0f}% uploaded...".format(pct), flush=True)

    print("  ✓ {} uploaded successfully.".format(filename), flush=True)


# ── MAIN ───────────────────────────────────────────────────

def main():
    print("\n" + "="*50)
    print("OneDrive Upload Started")
    print("Target folder: {}".format(ONEDRIVE_FOLDER))
    print("="*50)

    # Find CSV files to upload
    csv_files = sorted(Path(".").glob("telemetry_*.csv"))
    if not csv_files:
        print("No telemetry_*.csv files found — nothing to upload.")
        sys.exit(0)

    print("Files to upload:")
    for f in csv_files:
        size_mb = f.stat().st_size / 1024 / 1024
        print("  {} ({:.1f} MB)".format(f.name, size_mb))

    # Authenticate
    token   = get_access_token()
    user_id = get_user_id(token)

    # Ensure target folder exists
    ensure_folder(token, user_id, ONEDRIVE_FOLDER)

    # Upload each file
    failed = []
    for csv_path in csv_files:
        try:
            upload_file(token, user_id, str(csv_path), ONEDRIVE_FOLDER)
        except Exception as e:
            print("  ERROR uploading {}: {}".format(csv_path.name, e), flush=True)
            failed.append(csv_path.name)

    print("\n" + "="*50)
    if failed:
        print("Upload complete with errors. Failed files:")
        for f in failed:
            print("  - {}".format(f))
        sys.exit(1)
    else:
        print("All files uploaded successfully to OneDrive!")
        print("="*50)


if __name__ == "__main__":
    main()

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



# ==========================================================
# SITE GROUP CONFIGURATION
# ==========================================================

SITE_GROUP = os.getenv("SITE_GROUP", "ALL")

SITE_GROUPS = {

    "ALL": set(),

    "A": {
        "0042b802-1661-4af7-bae5-21b1db5a8b38",
        "00c73907-3d2c-4fa1-8693-579f208ae01d",
        "04e04ebf-633d-4156-93b0-41b64705e1e6",
        "05ad630c-383f-44b5-b8e0-7fb4347e3dc6",
        "072660ab-c588-4406-9468-72629b82f8cc",
        "0933fd88-d6a7-479e-b4b5-b7261f420d20",
        "0a5b6f30-5adf-4b06-8892-979f86ff0093",
        "0a88bc25-6c33-485c-ad6d-edbb5e7cfabc",
        "0b0338e9-97ac-4be0-a09a-04b7f88fda80",
        "0d57477a-1bdd-4aca-bce9-3a24a98c3bb2",
        "10437a9a-2ddf-4276-851b-af7dd1b1ee6e",
        "160617c6-8638-46df-b412-5951c211253a",
        "19d55e07-5801-46b5-8409-fdddc336bb1b",
        "1a8865bc-8a50-41bc-95c7-8ae21ba7d631",
        "1d95b0f6-812a-4fff-8a21-10bfdaba3e21",
        "1ee4c726-a1f3-4db0-87b6-0f1cc09bb1c2",
        "1f52437b-766c-41aa-bdbb-0c8182f58427",
        "1f5bc95f-3fa1-4237-ade9-c83d279df155",
        "200d195f-6119-4a23-8ea2-44e39dbf8cd7",
        "208fdad3-c1c2-4bbb-b2c8-8f55f0e41b4c",
        "213b268b-ceca-465a-afc9-02b8f8946edf",
        "217b3fcf-47a2-49c1-930b-b4e894d83e5e",
        "21d4a048-0cd0-4372-b3d3-1859b72d04d2",
        "22448569-6847-4add-95f7-1770519901fb",
        "24c1a2c9-db9a-4ca2-94d4-d017b4b00db1",
        "24eca80e-5df0-452d-87f3-d4565a6419a0",
        "2528c499-326b-4ac8-a8ff-e8c8a58bff96",
        "26ec9c95-93a9-475d-b956-bf77e1d84bdf",
        "27341c80-78da-4578-a954-e767788ce45f",
        "29414e83-d7f9-44c2-af3c-841e84d344bc",
        "2a61643f-389e-4e5c-9b09-7807cd8c3a77",
        "2b5e11d6-abd4-4db6-92d0-58c8e049455c",
        "2b8a8068-3838-445f-bd1a-527e1a6efb9c",
    },

    "B": {
        "2d222f61-1fd8-456b-81e4-9463efa8af83",
        "2f24a602-adcb-466f-bb6b-0508eda22204",
        "2f9fd897-dc62-485f-849c-dde30f149405",
        "3172c54d-a6dc-4cbd-9759-50dcb36937c3",
        "35b136d1-3aac-42b2-8933-397aad4cdf37",
        "36046dfe-651e-42df-94d7-2adb8679ee51",
        "361d61e6-cb8e-4e57-9d34-664d3d98b208",
        "3c27d23e-9480-4945-af10-22006aa1013d",
        "3f0e02a6-b495-4997-a30a-c5fbabacd089",
        "3f8660fd-80c1-4b9d-8c69-afb291fadf7b",
        "3fb34418-935a-4fe6-a7a2-a546abe1d0b1",
        "41241bb2-bd6a-4998-bdcf-43a32eba0643",
        "426f6a01-5199-447f-8fa3-6f92125acfa0",
        "43918f9c-bc7c-4809-b50b-9ccaba76748c",
        "4746477b-51ff-4b33-992e-bd7b307d31bf",
        "4978cda0-e7e9-4d71-940f-83b46ff0be1e",
        "4bb6d0b0-48b3-4a15-96e7-d629b331a36d",
        "4e39a0d8-c788-4f25-944d-e3d072f4d765",
        "4ec7a3a6-5911-4916-8929-90c9a5868756",
        "4ed3efb5-b0e2-43f8-a688-5706a894810c",
        "4fd92093-af2d-497c-b8cb-353d8c1ebc63",
        "55869b5c-fa30-4ef6-94d6-5053acf2242c",
        "5750a218-785e-4e55-8939-6dc3c3e11c35",
        "59a39e61-8586-42eb-be2d-bbe8e0b74d04",
        "5cdefac8-3dd7-46a0-8039-707137a87815",
        "637e89de-3c73-4024-aeb2-dd7ff14ec46c",
        "653ec1f3-f402-45b0-a530-9d5926a03293",
        "6548d2e1-9975-4b18-96ab-5047bfcf46db",
        "6813e46d-cbcb-47c2-a0eb-61f9cc0e216e",
        "6b0e094f-df01-47c6-ae74-f976adcfe0f3",
        "6b69fa35-2ccd-4266-afef-c5ef1c551edb",
        "6bc599b8-6956-45e2-a19d-9b410763c8f3",
        "6bcd6188-8e62-4eb7-bc13-26500cc6676c",
    },

    "C": {
        "6bdbe0f1-1950-45df-aa96-3a94c6284647",
        "6c9a108d-847d-4b05-af91-eb5d8d94a1c5",
        "6e296800-a847-462f-805c-06d1431dc11d",
        "6ebed25a-2ed7-4bb6-a8e0-212bb7b507f1",
        "70af4bba-91cb-48b8-aca8-d25f70424587",
        "71c90c2e-ee0c-450b-b877-651c8bb449a5",
        "731b8cf5-9ac7-456b-b4e1-b9912cceffdd",
        "7399a759-2a0c-4101-9ee6-ac6173ed7c69",
        "7588a550-2728-4856-8879-45f0858a9b11",
        "76b96354-aa61-4741-9ad6-d615873c5bbf",
        "76d162d8-295f-47e1-bf01-89f4e8c9b8bb",
        "77e9413c-946c-4eec-b15e-67514521d538",
        "781c3322-0511-4a86-b0c5-f0e41ee30fd3",
        "7971fd81-7bb4-42e1-9e34-f80bd0783d69",
        "7a42f4d6-a22c-4aa6-acb5-7480ac31f0d4",
        "7ab9a99e-6c11-45b4-acc2-93d0ad5d097f",
        "7cc7f0b2-0342-4764-94df-f49f0375f832",
        "81e02cad-d9f9-4787-9771-c2419658719d",
        "84201e7b-dfbf-4656-834f-e572d459fab7",
        "843589e7-0b2d-4b2e-94ba-2b89c1716034",
        "8539606e-50f7-436d-8199-c55c460efd8f",
        "85bfd6c9-440b-4842-bca1-d334aaa13fcc",
        "8ae21d05-9a84-40c2-aa86-ca637f9f9c9d",
        "8af00ec6-ec67-4df2-99d7-8bb10ad417d5",
        "8bed3b27-63ab-48c8-8955-4a7fe83deab7",
        "8e515973-5e43-4011-b2bd-f86d9ec86740",
        "9096f816-afd3-49b0-9e3a-898620e69ceb",
        "90da3afd-3341-4fe6-8ac8-15bad3dddd96",
        "90ffb945-94a2-4aa5-9a34-f59cec50d917",
        "91196233-296f-4da4-928f-83d035a824b5",
        "916faebb-ae7e-4fe5-8b82-9dcfc90744a9",
        "95b86457-a42f-47e7-bc7a-74415e2c540a",
        "9741ade8-9290-4371-b6e5-3f0a85599605",
    },

    "D": {
        "98ee0fe1-a4d3-4beb-bbdf-729718b650ea",
        "9a10e038-b8f1-4e83-a040-621c231df575",
        "9b74bf3d-42fd-4401-aa9f-65ac9c41ed66",
        "9cfbd362-892e-44e5-8611-050feb86c9f8",
        "9e984c81-1c35-4ee9-ab0d-8ea319c22889",
        "a019099a-ad7a-47d7-8e31-626e4b2886f0",
        "a0f3b952-0247-4fc0-8b4a-a0a8b9fb7176",
        "a50a0338-6cca-4a99-b42d-d84a9b64ac92",
        "a579a372-aade-4ca7-86a7-e19933b427d3",
        "a67a7457-ec72-4588-aeee-5d399f929b30",
        "a6d71c79-c1f7-46fa-844a-4a98b376a3e7",
        "a7b6d421-a2fd-497f-977d-9691918d4598",
        "a9d7ead3-4fa0-4639-a869-f9320ddbb129",
        "a9f37fbd-2cba-45d5-bb64-21589af26b98",
        "abe4cc0a-aa2a-42e8-9b13-91bd76a42c9a",
        "ad47e09c-d90c-46ee-a371-2d7824b6e7e3",
        "b5e0a56e-42bb-4b19-aaa3-39386604ddf4",
        "b65886dc-0cc2-4ce7-b2a7-6fac5a8cc4ce",
        "b8396912-791e-4dd7-a4ed-2a806c7be1a1",
        "b8abe2e7-40b7-47ec-9c11-0819560d4c28",
        "bcaa533c-9bac-45df-af3a-4f2e3494d415",
        "bd3da6bf-0984-44c0-8613-2d713d6dbef8",
        "be083c40-56c7-4b6b-9c3e-f7671c1663d4",
        "c192dc9d-2a6c-47a1-b87a-6e5681f52c75",
        "c244549c-1eb2-411c-90c9-4a15efe505d0",
        "c2bff0e3-ff1f-4b96-b1cf-ad1838ef0d33",
        "c63dd5fb-543f-4cd3-80fa-90738d0afe56",
        "c9004924-4714-4f11-9474-5159cd10da9b",
        "c96179ca-4627-4a49-8c6c-e3da6eed793b",
        "cd9df9da-df39-43a4-8a1c-2d2f1ac5dd37",
        "cddf5159-d984-43e9-998b-203f32450528",
        "d10494da-72cd-45c9-803b-2e6c289c9665",
        "d105752b-3378-40df-b496-a0f79941f23c",
    },

    "E": {
        "d6a2e385-531e-4ac4-9b57-63e78f774cce",
        "d790e311-4f24-4086-9c5a-a046f7a4db25",
        "d7bae7b2-99d7-4bff-9ac0-5257d651f078",
        "d7e835dc-f4c8-453e-8714-df0f23b82337",
        "dca09aec-52ab-4af7-a838-0e8d9cc644c9",
        "dcd9251a-0ef7-4aa5-9b81-daf7ef65d2bb",
        "dd038ad5-0ce0-4cb1-852d-a2332553abdb",
        "de52cac0-0cb7-4d18-9df0-9676b8567c12",
        "e07e6ef3-78d5-4ba6-8899-25b68df2bb80",
        "e507cda3-f659-4731-9235-57001710113a",
        "e56294b7-3736-405a-81e7-b4e6a48f29ce",
        "e5d7dc84-5930-4591-ab42-ff72a0630b35",
        "e61cb2fa-9951-4f44-9d61-4ef880052873",
        "e64f1983-fa0e-44a7-b906-56853820c600",
        "e94b0a50-442f-406d-ba55-5c052c28a494",
        "e9b06eb6-d249-49a5-9b9d-f0c369123836",
        "ea832dfb-95cb-4735-893b-010c52f9e117",
        "eb1ae0ff-2e04-476f-9031-4d2e63292965",
        "eb7856af-39ba-46d8-b9b9-4da497104deb",
        "ef625899-af50-4111-b306-d592493b92c9",
        "f1563496-c106-426a-9634-175dfbad6eb0",
        "f3635407-a5dd-4faf-8dd0-044ff768a555",
        "f3890424-efdc-44c1-a007-e7adf4b92c1c",
        "f48c88a0-6281-4df8-a5e0-295b3ada980a",
        "f4c62a87-d37c-412a-bbf5-77a488bb752a",
        "f632baee-93bd-473f-a70b-a8ff4da29c80",
        "f98b2142-b2d9-44b0-947f-b8ea0b283737",
        "fa589f54-c74f-4e90-9229-1b4aec6ec9db",
        "fa75862d-fbaf-4460-ba77-48d4d2beaa4c",
        "feffbe49-6fda-4d39-ba8a-150be1fd73ca",
        "ff640c6c-7e6f-4c04-a666-66398c42e428",
        "ffd0ba9f-f3ce-455e-9535-cd21d97e0489",
        "fff3ae44-cc01-4a68-b3b5-9ae95428a179",
    },

    "F": {
    },

    "G": {
    },

    "H": {
    }
}

EXCLUDED_SITES = {
    # Temporary exclusions go here
    # "dead-site-id"
}

STATE_ONEDRIVE_FOLDER = (
    "/Reliability/AWSDB/State"
)

ONEDRIVE_FOLDER = (
    "/Reliability/AWSDB/{SITE_GROUP}"
)

# ==========================================================
# FILE SETTINGS
# ==========================================================

STATE_FILE = f"aws_site_state_{SITE_GROUP}.json"

RETENTION_DAYS = 730
MAX_RUN_SECONDS = 19800

CHECKPOINT_INTERVAL = 900
STATE_SAVE_INTERVAL = 600

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
# GRAPH TOKEN
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

    return {
        "sites": {}
    }

def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ==========================================================
# AWS
# ==========================================================

def get_s3_client():

    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

# ==========================================================
# TIMESTAMP EXTRACTION
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
            return filename, index

        if path.stat().st_size < MAX_CSV_SIZE_BYTES:
            return filename, index

        index += 1

# ==========================================================
# DOWNLOAD EXISTING CSV FILES
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
# UPLOAD CSV
# ==========================================================

def upload_csv(token, local_path, delete_after=False):

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

    max_retries = 5

    for attempt in range(max_retries):

        try:

            with open(local_path, "rb") as f:

                response = requests.put(
                    upload_url,
                    headers=headers,
                    data=f,
                    timeout=600
                )

            if response.status_code in [200, 201]:

                print(
                    f"Uploaded {filename}",
                    flush=True
                )

                if delete_after:

                    try:

                        os.remove(local_path)

                        print(
                            f"Deleted local file "
                            f"{filename}",
                            flush=True
                        )

                    except Exception as delete_error:

                        print(
                            f"Could not delete "
                            f"{filename}: {delete_error}",
                            flush=True
                        )

                return

            if response.status_code == 409:

                wait_time = (attempt + 1) * 15

                print(
                    f"409 Conflict uploading "
                    f"{filename}. "
                    f"Retrying in {wait_time}s...",
                    flush=True
                )

                time.sleep(wait_time)

                continue

            response.raise_for_status()

        except Exception as e:

            if attempt == max_retries - 1:
                raise

            wait_time = (attempt + 1) * 15

            print(
                f"Retry upload for {filename} "
                f"in {wait_time}s due to: {e}",
                flush=True
            )

            time.sleep(wait_time)

    raise Exception(
        f"Failed upload after retries: {filename}"
    )

# ==========================================================
# CLEANUP
# ==========================================================

def cleanup_old_site_files(site_id, active_filename):

    for file in Path(".").glob(
        f"aws_telemetry_{site_id}_F*.csv"
    ):

        if str(file) != active_filename:

            try:

                os.remove(file)

                print(
                    f"Cleaned up old file "
                    f"{file.name}",
                    flush=True
                )

            except Exception as e:

                print(
                    f"Could not remove "
                    f"{file.name}: {e}",
                    flush=True
                )

# ==========================================================
# CHECKPOINT UPLOAD
# ==========================================================

def checkpoint_upload(
    token,
    active_site_files
):

    print(
        "\n=== CHECKPOINT UPLOAD START ===",
        flush=True
    )

    for site_id, filename in active_site_files.items():

        if not Path(filename).exists():
            continue

        try:

            upload_csv(
                token,
                filename
            )

        except Exception as e:

            print(
                f"ERROR uploading {filename}: {e}",
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
def download_state_file(token):

    remote_path = (
        f"{STATE_ONEDRIVE_FOLDER}/"
        f"{STATE_FILE}"
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
        timeout=60
    )

    if response.status_code == 200:

        with open(
            STATE_FILE,
            "wb"
        ) as f:

            f.write(response.content)

        print(
            f"Downloaded state file "
            f"{STATE_FILE}",
            flush=True
        )

    else:

        print(
            "No existing state file found.",
            flush=True
        )

def upload_state_file(token):

    remote_path = (
        f"{STATE_ONEDRIVE_FOLDER}/"
        f"{STATE_FILE}"
    )

    upload_url = (
        f"https://graph.microsoft.com/v1.0/"
        f"users/{ONEDRIVE_USER_EMAIL}"
        f"/drive/root:{remote_path}:/content"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    with open(
        STATE_FILE,
        "rb"
    ) as f:

        response = requests.put(
            upload_url,
            headers=headers,
            data=f,
            timeout=60
        )

    response.raise_for_status()

    print(
        f"Uploaded state file "
        f"{STATE_FILE}",
        flush=True
    )


        
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

    token = get_graph_token()

    download_state_file(token)

    state = load_state()

    active_site_files = {}

    s3 = get_s3_client()

    total_rows = 0
    processed_files = 0

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
                active_site_files
            )

            save_state(state)

            upload_state_file(token)

            return

        site_id = site_prefix.rstrip("/")

        included_sites = SITE_GROUPS.get(
            SITE_GROUP,
            set()
        )

        # If include list exists, only process those sites
        if included_sites:
            if site_id not in included_sites:
                continue

        # Exclude list always wins
        if site_id in EXCLUDED_SITES:
            continue
    
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

        previous_active_index = None

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
                        active_site_files
                    )

                    save_state(state)

                    upload_state_file(token)
                    

                    return

                key = obj["Key"]

                if not key.endswith(".csv.gz"):
                    continue

                file_timestamp = (
                    extract_timestamp_from_key(key)
                )

                if not file_timestamp:
                    continue

                if file_timestamp < cutoff_date:
                    continue

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

                    active_csv, active_index = (
                        get_active_csv(site_id)
                    )

                    if (
                        previous_active_index is not None
                        and active_index > previous_active_index
                    ):

                        finalized_file = (
                            f"aws_telemetry_{site_id}"
                            f"_F{previous_active_index}.csv"
                        )

                        print(
                            f"Uploading finalized file "
                            f"{finalized_file}",
                            flush=True
                        )

                        token = get_graph_token()

                        try:

                            upload_csv(
                                token,
                                finalized_file,
                                delete_after=True
                            )

                        except Exception as e:

                            print(
                                f"ERROR uploading finalized "
                                f"file: {e}",
                                flush=True
                            )

                    previous_active_index = active_index

                    active_site_files[site_id] = (
                        active_csv
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

                    state["sites"][site_id] = (
                        file_timestamp.isoformat()
                    )

                    if (
                        processed_files %
                        STATE_SAVE_INTERVAL
                    ) == 0:

                        save_state(state)

                        upload_state_file(token)

                    print(
                        f"  Added {len(df):,} rows "
                        f"to {active_csv}",
                        flush=True
                    )

                    if (
                        processed_files %
                        CHECKPOINT_INTERVAL
                    ) == 0:

                        token = get_graph_token()

                        checkpoint_upload(
                            token,
                            active_site_files
                        )

                        save_state(state)

                        upload_state_file(token)
                        

                except Exception as e:

                    print(
                        f"ERROR processing {key}: {e}",
                        flush=True
                    )

        print(
            f"Processed {file_counter} files for site",
            flush=True
        )

        if site_id in active_site_files:

            cleanup_old_site_files(
                site_id,
                active_site_files[site_id]
            )

    token = get_graph_token()

    checkpoint_upload(
        token,
        active_site_files
    )

    save_state(state)

    upload_state_file(token)

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
        f"Sites updated: {len(active_site_files):,}",
        flush=True
    )

    print("=" * 50, flush=True)

if __name__ == "__main__":
    main()

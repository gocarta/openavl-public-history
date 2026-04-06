# /// script
# dependencies = [
#   "boto3",
#   "datablob",
#   "duckdb",
#   "simple-env",
# ]
# ///

import boto3
import datablob
from datetime import datetime
import duckdb
import simple_env as se
from zoneinfo import ZoneInfo

RESULT_DATASET_NAME = "openavl_public_history"
RESULT_DATASET_VERSION = "1"

AWS_ACCESS_KEY_ID = se.get("AWS_ACCESS_KEY_ID")
if not AWS_ACCESS_KEY_ID:
    raise Exception("[openavl-public-history] missing AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = se.get("AWS_SECRET_ACCESS_KEY")
if not AWS_SECRET_ACCESS_KEY:
    raise Exception("[openavl-public-history] missing AWS_SECRET_ACCESS_KEY")

AWS_REGION = se.get("AWS_REGION")
if not AWS_REGION:
    raise Exception("[openavl-public-history] missing AWS_REGION")

AWS_BUCKET_NAME = se.get("AWS_BUCKET_NAME")
if not AWS_BUCKET_NAME:
    raise Exception("[openavl-public-history] missing AWS_BUCKET_NAME")

AWS_BUCKET_PATH_OUTPUT = se.get("AWS_BUCKET_PATH_OUTPUT")
if not AWS_BUCKET_PATH_OUTPUT:
    raise Exception("[openavl-public-history] missing AWS_BUCKET_PATH_OUTPUT")

OPENAVL_PUBLIC_VEHICLE_IDS = se.get("OPENAVL_PUBLIC_VEHICLE_IDS")
if not OPENAVL_PUBLIC_VEHICLE_IDS:
    raise Exception("[openavl-public-history] missing OPENAVL_PUBLIC_VEHICLE_IDS")

OPENAVL_DATA_SOURCE = se.get("OPENAVL_DATA_SOURCE")
if not OPENAVL_DATA_SOURCE:
    raise Exception("[openavl-public-history] missing OPENAVL_DATA_SOURCE")

OUTPUT_FILE = "data.parquet"

con = duckdb.connect()

con.execute(f"""
    CREATE SECRET (
        TYPE S3,
        KEY_ID '{AWS_ACCESS_KEY_ID}',
        SECRET '{AWS_SECRET_ACCESS_KEY}',
        REGION '{AWS_REGION}'
    );
""")

con.execute(f"""
    COPY (
        SELECT vehicle_id, timestamp, latitude, longitude, geometry
        FROM read_parquet('{OPENAVL_DATA_SOURCE}')
        WHERE vehicle_id IN ({OPENAVL_PUBLIC_VEHICLE_IDS})
    ) TO '{OUTPUT_FILE}' (FORMAT PARQUET);
""")

numRows = con.execute(f"SELECT count(*) FROM '{OUTPUT_FILE}'").fetchone()[0]

client = datablob.DataBlobClient(AWS_BUCKET_NAME, AWS_BUCKET_PATH_OUTPUT)

lastUpdated = dict(
    [(tz, datetime.now(ZoneInfo(tz)).isoformat()) for tz in ["America/New_York", "UTC"]]
)

metadata = {
    "name": RESULT_DATASET_NAME,
    "lastUpdated": lastUpdated,
    "description": "Historical Location of Almost Every CARTA Bus and Shuttle by the Second",
    "numColumns": 5,  # includes geometry columns
    "numRows": numRows,
    "columns": ["vehicle_id", "timestamp", "latitude", "longitude", "geometry"],
    "files": ["data.parquet"],
}

client.upload_metadata(RESULT_DATASET_NAME, RESULT_DATASET_VERSION, metadata)

parquet_key = (
    AWS_BUCKET_PATH_OUTPUT.strip("/")
    + "/"
    + RESULT_DATASET_NAME
    + "/v"
    + RESULT_DATASET_VERSION
    + "/data.parquet"
)
s3 = boto3.client("s3")

s3.upload_file("data.parquet", AWS_BUCKET_NAME, parquet_key)
print("[openavl-public-history] finished")

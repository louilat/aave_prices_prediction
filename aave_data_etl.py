"""Aave data extraction ETL"""

import boto3
import io
from dotenv import load_dotenv
import os
from src.reserves_features.reserves_features import (
    fetch_reserves_data,
    convert_units_and_get_hourly_granularity,
    fill_missing_data,
)

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

output_path = "aave-data/experiments/"

print("Step 1 - Extracting reserves' features")

print("   [1] - Querying Aave Protocol-v3 subgraph from Thegraph...")
reserves_table = fetch_reserves_data(
    1000, 300, verbose=True, timestamp_min=1727740800, timestamp_max=1730419199
)

print("   [2] - Cleaning reserves_table dataset (units and granularity)...")
reserves_history_hourly = convert_units_and_get_hourly_granularity(
    reserves_table=reserves_table
)

print("   [3] - Selecting main assets...")
assets_list = [
    "Wrapped Ether",
    "Wrapped BTC",
    "USD Coin",
    "Dai Stablecoin",
    "Wrapped liquid staked Ether 2.0",
    "Tether USD",
    "Aave Token",
]
reserves_history_hourly_selected_assets = reserves_history_hourly[
    reserves_history_hourly.reserve_name.isin(assets_list)
]

print("   [4] - Filling the missing rows with the latest available data...")
reserves_history_hourly_selected_assets_completed = fill_missing_data(
    reserves_history_hourly_selected_assets
)

print("Uploading files to s3...")
client_s3 = boto3.client(
    "s3",
    endpoint_url = 'https://'+'minio.lab.sspcloud.fr',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)

try:
    csv_buffer = io.StringIO()
    reserves_history_hourly_selected_assets_completed.to_csv(csv_buffer)
    client_s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket="llatournerie",
        Key=output_path + "reserves_history_hourly_selected_assets_completed.csv",
    )
    print(f"   --> Outputs successfully generated at {output_path}")
except Exception as e:
    print(f"Failed to upload files to s3, with error: {e}")

print("Done !")

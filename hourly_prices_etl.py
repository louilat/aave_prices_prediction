"""ETL for extracting prices data"""

import boto3
from datetime import datetime, timedelta, timezone
import io
import os
from dotenv import load_dotenv
from src.prices.prices_extraction_functions import (
    fetch_hourly_prices,
    clean_prices_data,
)
from src.utils.logger import Logger

logger = Logger()

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")

# Run parameters
output_path = "aave-data/data-prod/aave-v3/messari-prices/"
file_name = "hourly_prices"
year = 2024
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

api_endpoint = f"https://gateway.thegraph.com/api/{API_SECRET_KEY}/subgraphs/id/JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk"

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio.lab.sspcloud.fr",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)


logger.log("Starting ETL...")

for month in months:
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    next_date = start_date + timedelta(days=32)
    end_date = datetime(next_date.year, next_date.month, 1, tzinfo=timezone.utc)
    timestamp_min = datetime.timestamp(start_date)
    timestamp_max = datetime.timestamp(end_date)
    logger.log(f"min_date: {start_date}, max_date: {end_date}")
    logger.log(
        f"Starting data extraction with timestamp_min = {timestamp_min}, timestamp_max = {timestamp_max}"
    )

    logger.log("   --> STEP 1: Extracting raw data...")

    monthly_raw_prices = fetch_hourly_prices(
        api_endpoint=api_endpoint,
        size=1000,
        max_queries=100,
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
        verbose=True,
    )

    logger.log("   --> STEP 2: Cleaning data...")

    monthly_clean_prices = clean_prices_data(monthly_raw_prices)

    logger.log("   --> Uploading data to s3...")
    try:
        csv_buffer = io.StringIO()
        monthly_clean_prices.to_csv(csv_buffer, index=False)
        client_s3.put_object(
            Body=csv_buffer.getvalue(),
            Bucket="llatournerie",
            Key=output_path + file_name + f"_{year}_{month}.csv",
        )
    except Exception as e:
        logger.log(f"Failed to upload files to s3, with error: {e}")

client_s3.put_object(
    Body=logger.buffer.getvalue(),
    Bucket="llatournerie",
    Key=output_path + f"logfile_{year}.log",
)

logger.log("Done !")

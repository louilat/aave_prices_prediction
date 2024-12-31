"""Aave data extraction ETL"""

import boto3
import io
from dotenv import load_dotenv
import os
from datetime import datetime, timezone, timedelta

from src.reserves_features.reserves_features import (
    fetch_reserves_data,
    convert_units_and_get_hourly_granularity,
    fill_missing_data,
)

from src.reserves_features.reserves_features_quality_check import (
    reserve_data_quality_check,
    add_clean_data,
)

from src.utils.logger import Logger

logger = Logger()


load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")


# Run Parameters
output_path = "aave-data/data-prod/aave-v2/reserves-features/"
file_name = "reserves_history_hourly_selected_assets_completed"
version_2 = True
max_queries_number = 300
year = 2021
months_to_extract = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio.lab.sspcloud.fr",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)

logger.log("Starting ETL")

for month in months_to_extract:
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    next_date = start_date + timedelta(days=32)
    end_date = datetime(next_date.year, next_date.month, 1, tzinfo=timezone.utc)
    timestamp_min = datetime.timestamp(start_date)
    timestamp_max = datetime.timestamp(end_date)
    logger.log(f"min_date: {start_date}, max_date: {end_date}")
    logger.log(
        f"Starting data extraction with version_2 = {version_2} timestamp_min = {timestamp_min}, timestamp_max = {timestamp_max}"
    )

    logger.log("Step 1 - Extracting reserves' features")

    logger.log("   [1] - Querying Aave Protocol subgraph from Thegraph...")
    reserves_table = fetch_reserves_data(
        size=1000,
        n_iter=max_queries_number,
        version_2=version_2,
        verbose=True,
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
    )

    logger.log("   [2] - Cleaning reserves_table dataset (units and granularity)...")
    reserves_history_hourly = convert_units_and_get_hourly_granularity(
        reserves_table=reserves_table,
        version_2=version_2,
        logger=logger,
    )

    logger.log("   [3] - Selecting main assets...")
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

    logger.log("   [4] - Filling the missing rows with the latest available data...")
    reserves_history_hourly_selected_assets_completed = fill_missing_data(
        reserves_history_hourly_selected_assets,
        logger=logger,
    )

    logger.log("   [5] - Run quality checks...")
    try:
        outcome, score = reserve_data_quality_check(
            reserves_history_hourly_selected_assets_completed,
            version_2=version_2,
        )
        if not outcome:
            logger.log(f"   WARNING ! Quality check failed, with score: {score}")
        else:
            logger.log(f"   Passed quality check successfully, with score: {score}")
    except AssertionError as e:
        logger.log(f"   WARNING ! Quality check failed with FATAL ERROR: {e}")

    if version_2:
        logger.log("   [6] - Consolidate data...")
        reserves_history_hourly_selected_assets_completed = add_clean_data(
            reserves_history_hourly_selected_assets_completed,
            verbose=True,
            logger=logger,
        )

    logger.log("Uploading files to s3...")

    try:
        csv_buffer = io.StringIO()
        reserves_history_hourly_selected_assets_completed.to_csv(
            csv_buffer, index=False
        )
        client_s3.put_object(
            Body=csv_buffer.getvalue(),
            Bucket="llatournerie",
            Key=output_path + file_name + f"_{year}-{month}" + ".csv",
        )
        logger.log(f"   --> Outputs successfully generated at {output_path}")
    except Exception as e:
        logger.log(f"Failed to upload files to s3, with error: {e}")

client_s3.put_object(
    Body=logger.buffer.getvalue(),
    Bucket="llatournerie",
    Key=output_path + f"logfile_{year}.log",
)

logger.log("Done !")

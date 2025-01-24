"""ETL for extracting events data"""

import boto3
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import io
from src.events.events_extraction_functions import fetch_events, clean_events_data
from src.utils.logger import Logger

logger = Logger()

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")


# Run parameters
output_path = "aave-data/data-prod/aave-v2/events/"
file_name = "events_data"
year = 2024
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
version_2 = True
events_list = ["deposit", "borrow", "repay", "redeemUnderlying", "liquidationCall"]


if version_2:
    api_endpoint = f"https://gateway.thegraph.com/api/{API_SECRET_KEY}/subgraphs/id/8wR23o1zkS4gpLqLNU4kG3JHYVucqGyopL5utGxP2q1N"
else:
    api_endpoint = f"https://gateway.thegraph.com/api/{API_SECRET_KEY}/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"


client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio.lab.sspcloud.fr",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)


for month in months:
    # Compute month dates
    start_date = datetime(year, month, 1, tzinfo=timezone.utc)
    next_date = start_date + timedelta(days=32)
    end_date = datetime(next_date.year, next_date.month, 1, tzinfo=timezone.utc)
    timestamp_min = datetime.timestamp(start_date)
    timestamp_max = datetime.timestamp(end_date)
    logger.log(f"min_date: {start_date}, max_date: {end_date}")
    logger.log(
        f"Starting data extraction with version_2 = {version_2} timestamp_min = {timestamp_min}, timestamp_max = {timestamp_max}"
    )

    for event_name in events_list:
        logger.log(f"   --> Fetching {event_name} events...")
        raw_events = fetch_events(
            api_endpoint=api_endpoint,
            size=1000,
            max_queries=300,
            timestamp_min=timestamp_min,
            timestamp_max=timestamp_max,
            event=event_name,
            version_2=version_2,
            verbose=True,
        )

        logger.log(len(raw_events))

        logger.log(f"   --> Cleaning {event_name} events...")
        clean_events = clean_events_data(
            event_name=event_name,
            events_data=raw_events,
            logger=logger,
        )
        logger.log(f"   --> INFO: Found {len(clean_events)} events")

        try:
            csv_buffer = io.StringIO()
            clean_events.to_csv(csv_buffer, index=False)
            client_s3.put_object(
                Body=csv_buffer.getvalue(),
                Bucket="llatournerie",
                Key=output_path
                + f"{event_name}/"
                + file_name
                + f"_{event_name}_{year}-{month}"
                + ".csv",
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

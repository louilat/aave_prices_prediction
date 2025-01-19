"""ETL for combining users' balances (vtokens and atokens) with the events data"""

import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
from src.utils.logger import Logger
from src.users_balances.users_balances_processing_functions import (
    extract_events_data,
    combine_atokens_vtokens_balances,
    clean_events,
    clean_liquidation_events,
    match_balances_with_events,
)

logger = Logger()

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio.lab.sspcloud.fr",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
)

# Run parameters
balances_inputs_path = "aave-data/data-prod/aave-v3/users-positions/"
events_inputs_path = "aave-data/data-prod/aave-v3/events/"
output_path = "aave-data/data-prod/aave-v3/users-positions-combined/"
year = 2024
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

logger.log("Starting ETL...")

for month in months:
    logger.log(f"Treating year={year}, month={month}")
    logger.log("   --> Extracting events and balances data...")
    month_atoken_balances_path = (
        "https://minio.lab.sspcloud.fr/llatournerie/"
        + balances_inputs_path
        + f"users_atoken_balances_{year}-{month}.csv"
    )
    month_vtoken_balances_path = (
        "https://minio.lab.sspcloud.fr/llatournerie/"
        + balances_inputs_path
        + f"users_vtoken_balances_{year}-{month}.csv"
    )
    month_events_path = (
        "https://minio.lab.sspcloud.fr/llatournerie/" + events_inputs_path
    )

    atokens = pd.read_csv(month_atoken_balances_path)
    vtokens = pd.read_csv(month_vtoken_balances_path)
    interaction_events, liquidation_events = extract_events_data(
        month_events_path, year, month
    )

    logger.log("   --> Combining atokens and vtokens...")
    clean_balances = combine_atokens_vtokens_balances(atokens=atokens, vtokens=vtokens)

    logger.log("   --> Cleaning interaction events...")
    interaction_events_clean = clean_events(events=interaction_events)

    logger.log("   --> Cleaning liquidation events...")
    liquidation_events_clean = clean_liquidation_events(liquidations=liquidation_events)

    logger.log("   --> Matching balances and events...")
    combined_data = match_balances_with_events(
        combined_atokens_vtokens_balances=clean_balances,
        clean_interaction_events=interaction_events_clean,
        clean_liquidation_events=liquidation_events_clean,
    )

    missing_events_ratio = len(combined_data[combined_data.action.isna()]) / len(
        combined_data
    )
    logger.log(f"   INFO: Fraction of unmatched balances: {missing_events_ratio}")

    logger.log("   --> Uploading output to s3...")
    csv_buffer = io.StringIO()
    combined_data.to_csv(csv_buffer, index=False)
    client_s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket="llatournerie",
        Key=output_path + f"combined_balances_{year}-{month}.csv",
    )

logger.log("Done!")

client_s3.put_object(
    Body=logger.buffer.getvalue(),
    Bucket="llatournerie",
    Key=output_path + f"logfile_{year}.log",
)

import boto3
import io
import os
from dotenv import load_dotenv
import concurrent.futures
from src.users_balances.users_balances_extraction_functions import (
    extract_monthly_users_data,
)
from src.utils.logger import Logger

global_logger = Logger()

load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")


# Run Parameters
output_path = "aave-data/data-prod/aave-v2/users-positions/"
year = 2024
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
version_2 = True

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

global_logger.log("Extracting ATokens data...")

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_month = {
        executor.submit(
            extract_monthly_users_data,
            api_endpoint,
            1000,
            300,
            year,
            month,
            "atoken",
            version_2,
            True,
        ): f"{year}-{month}"
        for month in months
    }

    for future in concurrent.futures.as_completed(future_to_month):
        month_string = future_to_month[future]
        try:
            # Extract outputs
            atoken_balances = future.result()

            # Write to s3
            csv_buffer = io.StringIO()
            atoken_balances.to_csv(csv_buffer, index=False)
            client_s3.put_object(
                Body=csv_buffer.getvalue(),
                Bucket="llatournerie",
                Key=output_path + f"users_atoken_balances_{month_string}.csv",
            )
        except Exception as e:
            global_logger.log(f"Generated an exeption for {month_string}: {e}")


global_logger.log("Extracting VTokens data...")

with concurrent.futures.ThreadPoolExecutor() as executor:
    future_to_month = {
        executor.submit(
            extract_monthly_users_data,
            api_endpoint,
            1000,
            300,
            year,
            month,
            "vtoken",
            version_2,
            True,
        ): f"{year}-{month}"
        for month in months
    }

    for future in concurrent.futures.as_completed(future_to_month):
        month_string = future_to_month[future]
        try:
            # Extract outputs
            vtoken_balances = future.result()

            # Write to s3
            csv_buffer = io.StringIO()
            vtoken_balances.to_csv(csv_buffer, index=False)
            client_s3.put_object(
                Body=csv_buffer.getvalue(),
                Bucket="llatournerie",
                Key=output_path + f"users_vtoken_balances_{month_string}.csv",
            )
        except Exception as e:
            global_logger.log(f"Generated an exeption for {month_string}: {e}")

client_s3.put_object(
    Body=global_logger.buffer.getvalue(),
    Bucket="llatournerie",
    Key=output_path + f"globallogfile_{year}.log",
)

global_logger.log("Done!")

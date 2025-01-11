"""Functions for extracting the users' balances"""

import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
from io import StringIO
from ..utils.utils import run_query
from ..utils.logger import Logger


def run_query_users_balances_protocol_v3(
    api_endpoint: str,
    size: int,
    offset: int,
    timestamp_min: int,
    timestamp_max: int,
    token: str,
) -> dict:
    if token == "atoken":
        query_name = "atokenBalanceHistoryItems"
        scaled_balance = "scaledATokenBalance"
        current_balance = "currentATokenBalance"
    elif token == "vtoken":
        query_name = "vtokenBalanceHistoryItems"
        scaled_balance = "scaledVariableDebt"
        current_balance = "currentVariableDebt"
    else:
        raise ValueError(
            f"Undefined token value, expected atoken or vtoken, got {token}"
        )
    query = f"""\u007b
        {query_name}(
            first: {size}
            skip: {offset}
            orderBy: timestamp
            orderDirection: desc
            where: \u007b
                timestamp_gt: {timestamp_min}
                timestamp_lt: {timestamp_max}
            \u007d
        ) \u007b
            id
            timestamp
            {scaled_balance}
            {current_balance}
            index
            userReserve \u007b
                reserve \u007b
                    name
                    decimals
                    usageAsCollateralEnabled
                \u007d
                user \u007b
                    id
                \u007d
                pool \u007b
                    pool
                \u007d
            \u007d
        \u007d
    \u007d
    """
    return run_query(api=api_endpoint, query=query)


def fetch_users_balances(
    api_endpoint: str,
    size: int,
    max_queries: int,
    timestamp_min: int,
    timestamp_max: int,
    token: str,
    verbose: bool = False,
):
    if token == "atoken":
        response_key = "atokenBalanceHistoryItems"
    elif token == "vtoken":
        response_key = "vtokenBalanceHistoryItems"
    else:
        raise ValueError(
            f"Undefined token value, expected atoken or vtoken, got {token}"
        )
    users_balances = pd.DataFrame()
    for iter in range(max_queries):
        if verbose:
            if iter % 10 == 0:
                print(f"      [Iteration {iter + 1}/{max_queries}]")
        query_output = run_query_users_balances_protocol_v3(
            api_endpoint=api_endpoint,
            size=size,
            offset=iter * size,
            timestamp_min=timestamp_min,
            timestamp_max=timestamp_max,
            token=token,
        )
        current_balances = pd.json_normalize(query_output["data"][response_key])
        if len(current_balances) == 0:
            print("All data has been already extracted")
            break
        users_balances = pd.concat((users_balances, current_balances))
    return users_balances


def clean_users_balances_data(users_balances: DataFrame, token: str) -> DataFrame:
    clean_users_balances = users_balances.rename(
        columns={
            "userReserve.user.id": "user_address",
            "userReserve.pool.pool": "pool",
            "userReserve.reserve.decimals": "reserve_decimals",
            "userReserve.reserve.name": "reserve_name",
            "userReserve.reserve.usageAsCollateralEnabled": "usage_as_collateral_enabled",
            "index": "Index",
        }
    )

    clean_users_balances.reserve_decimals = clean_users_balances.reserve_decimals.apply(
        int
    )
    clean_users_balances.Index = clean_users_balances.Index.apply(int) / 1e27

    if token == "atoken":
        clean_users_balances = clean_users_balances.rename(
            columns={
                "currentATokenBalance": "user_current_atoken_balance",
                "scaledATokenBalance": "user_scaled_atoken_balance",
            }
        )
        clean_users_balances.user_current_atoken_balance = (
            clean_users_balances.user_current_atoken_balance.apply(int)
            / 10**clean_users_balances.reserve_decimals
        )
        clean_users_balances.user_scaled_atoken_balance = (
            clean_users_balances.user_scaled_atoken_balance.apply(int)
            / 10**clean_users_balances.reserve_decimals
        )

    elif token == "vtoken":
        clean_users_balances = clean_users_balances.rename(
            columns={
                "scaledVariableDebt": "user_scaled_variable_debt",
                "currentVariableDebt": "user_current_variable_debt",
            }
        )
        clean_users_balances.user_current_variable_debt = (
            clean_users_balances.user_current_variable_debt.apply(int)
            / 10**clean_users_balances.reserve_decimals
        )
        clean_users_balances.user_scaled_variable_debt = (
            clean_users_balances.user_scaled_variable_debt.apply(int)
            / 10**clean_users_balances.reserve_decimals
        )

    else:
        raise ValueError(
            f"Undefined token value, expected atoken or vtoken, got {token}"
        )

    return clean_users_balances.drop_duplicates()


def extract_monthly_users_data(
    api_endpoint: str,
    size: int,
    max_queries: int,
    year: int,
    month: int,
    token: str,
    verbose: bool = False,
) -> tuple[DataFrame, StringIO]:
    logger = Logger()
    logger.log(f"Starting users balances ETL for year={year}, month={month}")
    # Computing beginning and end of month in timestamp format
    start_datetime = datetime(year, month, 1)
    end_datetime = start_datetime + timedelta(days=32)
    end_datetime = datetime(end_datetime.year, end_datetime.month, 1)
    timestamp_min = start_datetime.timestamp()
    timestamp_max = end_datetime.timestamp()
    if verbose:
        logger.log(
            f"   --> Starting datetime: {start_datetime}, Timestamp: {timestamp_min}"
        )
        logger.log(
            f"   --> Ending datetime: {end_datetime}, Timestamp: {timestamp_max}"
        )

    logger.log("[STEP1] Fetching users balances raw data...")
    users_balances = fetch_users_balances(
        api_endpoint=api_endpoint,
        size=size,
        max_queries=max_queries,
        timestamp_min=timestamp_min,
        timestamp_max=timestamp_max,
        token=token,
        verbose=verbose,
    )

    logger.log("[STEP 2] Cleaning users balances data...")
    clean_users_balances = clean_users_balances_data(
        users_balances=users_balances, token=token
    )

    logger.log("Done!")

    return clean_users_balances, logger.buffer

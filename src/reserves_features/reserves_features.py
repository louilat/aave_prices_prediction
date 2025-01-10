"""Functions for extracting the Aave's reserves data"""

from dotenv import load_dotenv
import os
import pandas as pd
from pandas import DataFrame
import numpy as np
from ..utils.logger import Logger
from ..utils.utils import run_query

load_dotenv()

API_SECRET_KEY = os.getenv("API_SECRET_KEY")

api_endpoint_v3 = f"https://gateway.thegraph.com/api/{API_SECRET_KEY}/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
api_endpoint_v2 = f"https://gateway.thegraph.com/api/{API_SECRET_KEY}/subgraphs/id/8wR23o1zkS4gpLqLNU4kG3JHYVucqGyopL5utGxP2q1N"


outliers_index_threshold = {
    "Wrapped Ether": 5e-3,
    "Wrapped BTC": 5e-3,
    "USD Coin": 5e-3,
    "Dai Stablecoin": 2.5e-3,
    "Wrapped liquid staked Ether 2.0": 5e-3,
    "Tether USD": 5e-3,
    "Aave Token": 5e-3,
}


def run_query_reserves_statistics_protocol_v3(
    size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""
        \u007b
            reserveParamsHistoryItems(
                first: {size},
                skip: {offset},
                where: \u007b timestamp_gt: {timestamp_min}, timestamp_lt: {timestamp_max} \u007d
            ) \u007b
                reserve \u007b
                    name
                    pool \u007b
                        pool
                    \u007d
                    decimals
                \u007d
                timestamp
                variableBorrowRate
                variableBorrowIndex
                stableBorrowRate
                averageStableBorrowRate
                liquidityIndex
                liquidityRate
                totalLiquidity
                totalATokenSupply
                availableLiquidity
                totalCurrentVariableDebt
                totalScaledVariableDebt
                totalPrincipalStableDebt
                utilizationRate
                accruedToTreasury
                priceInEth
                priceInUsd
            \u007d
        \u007d
    """
    return run_query(api_endpoint_v3, query)


def run_query_reserves_statistics_protocol_v2(
    size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""
        \u007b
            reserveParamsHistoryItems(
                first: {size},
                skip: {offset},
                where: \u007b timestamp_gt: {timestamp_min}, timestamp_lt: {timestamp_max} \u007d
            ) \u007b
                reserve \u007b
                    name
                    decimals
                \u007d
                timestamp
                variableBorrowRate
                variableBorrowIndex
                stableBorrowRate
                averageStableBorrowRate
                liquidityIndex
                liquidityRate
                totalLiquidity
                totalATokenSupply
                availableLiquidity
                totalCurrentVariableDebt
                totalScaledVariableDebt
                totalPrincipalStableDebt
                utilizationRate
                priceInEth
                priceInUsd
            \u007d
        \u007d
    """
    return run_query(api_endpoint_v2, query)


def fetch_reserves_data(
    size: int,
    n_iter: int,
    timestamp_min: int,
    timestamp_max: int,
    version_2: bool = False,
    verbose: bool = False,
) -> DataFrame:
    reserves_table = pd.DataFrame()
    for iter in range(n_iter):
        if verbose:
            if iter % 10 == 0:
                print(f"      [Iteration {iter + 1}/{n_iter}]")
        if version_2:
            query_output = run_query_reserves_statistics_protocol_v2(
                size=size,
                offset=iter * size,
                timestamp_min=timestamp_min,
                timestamp_max=timestamp_max,
            )
        else:
            query_output = run_query_reserves_statistics_protocol_v3(
                size=size,
                offset=iter * size,
                timestamp_min=timestamp_min,
                timestamp_max=timestamp_max,
            )
        current_reserves_table = pd.json_normalize(
            query_output["data"]["reserveParamsHistoryItems"]
        )
        if len(current_reserves_table) == 0:
            print("All data has been already extracted")
            break
        reserves_table = pd.concat((reserves_table, current_reserves_table))
    return reserves_table


def convert_units(
    reserves_table: DataFrame,
    logger: Logger,
    version_2: bool = False,
    verbose: bool = True,
) -> DataFrame:
    reserves_history = reserves_table.copy()
    reserves_history = reserves_history.reset_index(drop=True)
    reserves_history = reserves_history.drop_duplicates()
    if verbose:
        logger.log(
            f"      --> Dropped {len(reserves_table) - len(reserves_history)} duplicates: total of {len(reserves_history)} rows"
        )

    reserves_history = reserves_history.rename(
        columns={
            "reserve.decimals": "reserve_decimals",
            "reserve.name": "reserve_name",
            "reserve.pool.pool": "reserve_pool",
        }
    )

    if version_2:
        columns_types = {
            "timestamp": np.int64,
            "utilizationRate": float,
            "reserve_name": str,
        }
    else:
        columns_types = {
            "timestamp": np.int64,
            "utilizationRate": float,
            "reserve_name": str,
            "reserve_pool": str,
        }

    reserves_history = reserves_history.astype(columns_types)

    # Change units
    reserves_history.reserve_decimals = reserves_history.reserve_decimals.apply(int)

    if not version_2:
        reserves_history.accruedToTreasury = (
            reserves_history.accruedToTreasury.apply(int)
            / 10**reserves_history.reserve_decimals
        )

    reserves_history.availableLiquidity = (
        reserves_history.availableLiquidity.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.averageStableBorrowRate = (
        reserves_history.averageStableBorrowRate.apply(int) * 1e-27
    )
    reserves_history.liquidityIndex = reserves_history.liquidityIndex.apply(int) * 1e-27
    reserves_history.liquidityRate = reserves_history.liquidityRate.apply(int) * 1e-27
    reserves_history.priceInEth = reserves_history.priceInEth.apply(int)
    reserves_history.stableBorrowRate = (
        reserves_history.stableBorrowRate.apply(int) * 1e-27
    )

    reserves_history.totalATokenSupply = (
        reserves_history.totalATokenSupply.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.totalCurrentVariableDebt = (
        reserves_history.totalCurrentVariableDebt.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.totalLiquidity = (
        reserves_history.totalLiquidity.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.totalPrincipalStableDebt = (
        reserves_history.totalPrincipalStableDebt.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.totalScaledVariableDebt = (
        reserves_history.totalScaledVariableDebt.apply(int)
        / 10**reserves_history.reserve_decimals
    )
    reserves_history.variableBorrowIndex = (
        reserves_history.variableBorrowIndex.apply(int) * 1e-27
    )
    reserves_history.variableBorrowRate = (
        reserves_history.variableBorrowRate.apply(int) * 1e-27
    )

    return reserves_history


def get_hourly_granularity(
    reserves_table: DataFrame,
    logger: Logger,
    version_2: bool = False,
    verbose: bool = True,
) -> DataFrame:
    reserves_history = reserves_table.copy()
    reserves_history["datetime"] = pd.to_datetime(
        reserves_history.timestamp, unit="s", utc=True
    ).dt.floor("h")

    if version_2:
        reserves_history_ = DataFrame()
        for reserve_name in reserves_history.reserve_name.unique().tolist():
            reserve_asset = reserves_history[
                reserves_history.reserve_name == reserve_name
            ]
            reserve_asset = reserve_asset.sort_values(["datetime"])
            reserve_asset = flag_outlier_indexes(
                reserves_data=reserve_asset,
                column_name="liquidityIndex",
                variation_threshold=outliers_index_threshold[reserve_name],
            )
            reserve_asset = flag_outlier_indexes(
                reserves_data=reserve_asset,
                column_name="variableBorrowIndex",
                variation_threshold=outliers_index_threshold[reserve_name],
            )
            reserves_history_ = pd.concat((reserves_history_, reserve_asset))
        assert len(reserves_history_) == len(reserves_history)
        reserves_history = reserves_history_
        reserves_history = reserves_history[
            reserves_history.is_not_outlier_liquidityIndex
        ]
        reserves_history = reserves_history[
            reserves_history.is_not_outlier_variableBorrowIndex
        ]
        reserves_history = reserves_history.drop(
            columns=[
                "is_not_outlier_liquidityIndex",
                "is_not_outlier_variableBorrowIndex",
            ]
        )
        if verbose:
            logger.log(
                f"      --> Dropped {len(reserves_history_) - len(reserves_history)} lines when removing indexes outliers"
            )

    # Keep Hour Granularity
    reserves_history_last_data_per_hour = reserves_history.groupby(
        ["reserve_name", "datetime"]
    )["timestamp"].transform("max")
    reserves_history_last_data_per_hour_mask = (
        reserves_history.timestamp == reserves_history_last_data_per_hour
    )
    reserves_history_hourly = reserves_history[reserves_history_last_data_per_hour_mask]
    reserves_history_hourly = reserves_history_hourly.drop_duplicates(
        subset=["reserve_name", "datetime"]
    )
    if verbose:
        logger.log(
            f"      --> Dropped {len(reserves_history) - len(reserves_history_hourly)} rows when getting the hour granularity"
        )
        logger.log(f"      --> Total of {len(reserves_history_hourly)} rows")
    return reserves_history_hourly


def flag_outlier_indexes(
    reserves_data: DataFrame,
    column_name: str,
    variation_threshold: float,
    start: int = 1,
    force: bool = False,
) -> DataFrame:
    """
    Returs a dataframe similar to `reserves_data` with an extra column
    that flags the outliers with False.

    Args:
        reserves_data (DataFrame): The dataframe containing the index outliers
        column_name (str): The column of reserves_data to process
        variation_threshold (float): The maximum delta between two consecutive values above which
            the next data is considered as an outlier.
        fill_values (bool, default to True):
    Returns:
        DataFrame: The reserves_data dataframe with an extra column named `is_not_outlier_{column_name}`
            that flags outliers.
    """
    fixed_reserves_data = reserves_data.sort_values("timestamp").reset_index(drop=True)
    if start >= 7:
        return flag_outlier_indexes(
            reserves_data,
            column_name,
            variation_threshold,
            start=0,
            force=True,
        )

    n = len(fixed_reserves_data)
    index_data = fixed_reserves_data[column_name]
    filter_mask = np.ones(n, dtype=bool)
    filter_mask[:start] = False
    last_valid_value = index_data[start]
    for current_position in range(start + 1, n):
        current_value = index_data[current_position]
        if (current_value < last_valid_value) or (
            abs(current_value - last_valid_value) > variation_threshold
        ):
            filter_mask[current_position] = False
        else:
            last_valid_value = current_value

    # Remove
    if np.sum(filter_mask) > 15 or force:
        fixed_reserves_data[f"is_not_outlier_{column_name}"] = filter_mask
        return fixed_reserves_data
    else:
        return flag_outlier_indexes(
            reserves_data,
            column_name,
            variation_threshold,
            start=start + 1,
            force=False,
        )


def fill_missing_data(
    hourly_reserves_snapshots: DataFrame, logger: Logger, verbose: bool = True
) -> DataFrame:
    starting_datetime = min(pd.to_datetime(hourly_reserves_snapshots.datetime))
    ending_datetime = max(pd.to_datetime(hourly_reserves_snapshots.datetime))
    if verbose:
        logger.log(f"      --> Minimum datetime: {starting_datetime}")
        logger.log(f"      --> Maximum datetime: {ending_datetime}")
    reserves_list = hourly_reserves_snapshots.reserve_name.unique()
    regular_outputs_list = list()
    # current_hour = starting_datetime
    delta_hours = int((ending_datetime - starting_datetime).total_seconds() / 3600)
    output_datetimes_list = [
        starting_datetime + pd.Timedelta(hours=k) for k in range(delta_hours + 1)
    ]
    base_output = DataFrame({"regular_datetime": output_datetimes_list})
    for reserve in reserves_list:
        if verbose:
            logger.log(f"      --> Reserve_name: {reserve}")
        reserve_data = hourly_reserves_snapshots[
            hourly_reserves_snapshots.reserve_name == reserve
        ]
        assert len(reserve_data.datetime.unique()) == len(
            reserve_data.datetime
        ), "Same datetime appears several times for a given asset"
        if verbose:
            logger.log(
                f"      --> {len(base_output) - len(reserve_data)} rows are missing"
            )
        # reserve_data = reserve_data.sort_values("datetime").reset_index(drop=True)
        reserve_output = base_output.merge(
            reserve_data, how="left", left_on="regular_datetime", right_on="datetime"
        )
        reserve_output = reserve_output.infer_objects(copy=False).ffill()
        reserve_output.reserve_name = reserve

        # Add a "filled_value" flag
        true_values = DataFrame(
            {"regular_datetime": reserve_data.datetime, "true_value": 1}
        )
        reserve_output = reserve_output.merge(
            true_values, how="left", on="regular_datetime"
        )
        reserve_output[["true_value"]] = reserve_output[["true_value"]].fillna(value=0)
        reserve_output = reserve_output.drop(columns="datetime")
        regular_outputs_list.append(reserve_output)
    return pd.concat(regular_outputs_list)

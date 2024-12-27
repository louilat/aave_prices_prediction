"""Functions for extracting the Aave's reserves data"""

from dotenv import load_dotenv
import os
import pandas as pd
from pandas import DataFrame
import numpy as np
from ..utils.utils import run_query

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY")

api_endpoint_v3 = f"https://gateway.thegraph.com/api/{SECRET_KEY}/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
api_endpoint_v2 = f"https://gateway.thegraph.com/api/{SECRET_KEY}/subgraphs/id/8wR23o1zkS4gpLqLNU4kG3JHYVucqGyopL5utGxP2q1N"


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


def convert_units_and_get_hourly_granularity(
    reserves_table: DataFrame, version_2: bool = False, verbose: bool = True
) -> DataFrame:
    reserves_history = reserves_table.copy()
    reserves_history = reserves_history.reset_index(drop=True)
    reserves_history = reserves_history.drop_duplicates()
    if verbose:
        print(
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

    # Keep Hour Granularity
    reserves_history["datetime"] = pd.to_datetime(reserves_history.timestamp, unit="s")
    reserves_history.datetime = reserves_history.datetime.dt.round("h")
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
        print(
            f"      --> Dropped {len(reserves_history) - len(reserves_history_hourly)} rows when getting the hour granularity"
        )
        print(f"      -->Total of {len(reserves_history_hourly)} rows")
    return reserves_history_hourly


def fill_missing_data(
    hourly_reserves_snapshots: DataFrame, verbose: bool = True
) -> DataFrame:
    starting_datetime = min(pd.to_datetime(hourly_reserves_snapshots.datetime))
    ending_datetime = max(pd.to_datetime(hourly_reserves_snapshots.datetime))
    if verbose:
        print(f"      --> Minimum datetime: {starting_datetime}")
        print(f"      --> Maximum datetime: {ending_datetime}")
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
            print(f"      --> Reserve_name: {reserve}")
        reserve_data = hourly_reserves_snapshots[
            hourly_reserves_snapshots.reserve_name == reserve
        ]
        assert len(reserve_data.datetime.unique()) == len(
            reserve_data.datetime
        ), "Same datetime appears several times for a given asset"
        if verbose:
            print(f"      --> {len(base_output) - len(reserve_data)} rows are missing")
        # reserve_data = reserve_data.sort_values("datetime").reset_index(drop=True)
        reserve_output = base_output.merge(
            reserve_data, how="left", left_on="regular_datetime", right_on="datetime"
        )
        reserve_output = reserve_output.infer_objects(copy=False).ffill()

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

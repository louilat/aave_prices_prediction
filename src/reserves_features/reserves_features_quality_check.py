"""Data quality check functions for the reserves features extraction"""

import pandas as pd
from pandas import DataFrame, Timedelta
import numpy as np
from ..utils.logger import Logger

logger = Logger()


def reserve_data_quality_check(
    hourly_asset_reserve_completed: DataFrame,
    version_2: bool = False,
) -> tuple[bool, float]:
    """Assesses the quality of the extracted data by checking the following conditions:
    1. The size of the dataframe should be either 30*24, 31*24, 28*24 or 29*24 (fatal)
    2. No duplicates (fatal)
    2. The variable borrow and liquidity indexes should be ingreasing, higher than 1
    3. The variable borrow, liquidity and utilization rates should lie between 0 and 1
    4. The balance sheet equilibrium should be 'close' to 0

    Args:
        hourly_asset_reserve_completed (DataFrame): The hourly
            reserve data (completed with previous hours) to check.
    Returns:
        bool: True if the score is higher than 85%, else False
        float: The quality score
    """
    # Conditions 1 and 2
    valid_lengths = [28 * 24, 29 * 24, 30 * 24, 31 * 24]
    l = len(hourly_asset_reserve_completed)
    assert l in valid_lengths, f"Invalid length: {l}"
    assert len(hourly_asset_reserve_completed.drop_duplicates()) == len(
        hourly_asset_reserve_completed
    ), "Found duplicates"

    # Condition 3
    borrow_index_mask = (
        hourly_asset_reserve_completed.variableBorrowIndex
        == hourly_asset_reserve_completed.variableBorrowIndex.cummax()
    ) & (hourly_asset_reserve_completed.variableBorrowIndex >= 1)

    liquidity_index_mask = (
        hourly_asset_reserve_completed.liquidityIndex
        == hourly_asset_reserve_completed.liquidityIndex.cummax()
    ) & (hourly_asset_reserve_completed.liquidityIndex >= 1)

    index_score = (np.mean(borrow_index_mask) + np.mean(liquidity_index_mask)) / 2

    # Condition 4
    borrow_rate_mask = (hourly_asset_reserve_completed.variableBorrowRate >= 0) & (
        hourly_asset_reserve_completed.variableBorrowRate <= 1
    )
    liquidity_rate_mask = (hourly_asset_reserve_completed.liquidityRate >= 0) & (
        hourly_asset_reserve_completed.liquidityRate <= 1
    )

    rate_score = (np.mean(borrow_rate_mask) + np.mean(liquidity_rate_mask)) / 2

    # Condition 5
    if not version_2:
        balance_equilibrium_threshold = 0.05 * (
            hourly_asset_reserve_completed.totalATokenSupply
            + hourly_asset_reserve_completed.accruedToTreasury
        )
        balance_equilibrium = (
            hourly_asset_reserve_completed.totalATokenSupply
            + hourly_asset_reserve_completed.accruedToTreasury
            - hourly_asset_reserve_completed.availableLiquidity
            - hourly_asset_reserve_completed.totalCurrentVariableDebt
        )

        balance_score = np.mean(
            np.abs(balance_equilibrium) <= balance_equilibrium_threshold
        )

        quality_score = float((index_score + rate_score + balance_score) / 3)

        return quality_score > 0.95, quality_score

    else:
        quality_score = float((index_score + rate_score) / 2)
        return quality_score > 0.95, quality_score


def add_clean_data_per_asset(hourly_asset_reserve_completed: DataFrame) -> DataFrame:
    """
    Add extra columns to the hourly_asset_reserve_completed with consolidated values.
    The extra columns are:
        fixed_variableBorrowIndex: cummax of variableBorrowIndex
        fixed_liquidityIndex: cummax of liquidityIndex
        fixed_variableBorrowRate: variableBorrowRate capped between 0 and 1
        fixed_liquidityRate: liquidityrate capped between 0 and 1
        fixed_utilizationRate: utilizationrate capped between 0 and 1

    Args:
        hourly_asset_reserve_completed (DataFrame): The table to complete with fixed values
        verbose (bool): Whether to print logs, default to False
    Returns:
        DataFrame: The input table with extra columns for fixed values
    """

    clean_reserve_data = remove_indexes_outliers(
        reserve_data=hourly_asset_reserve_completed,
        index_column="liquidityIndex",
    )

    clean_reserve_data = remove_indexes_outliers(
        reserve_data=clean_reserve_data,
        index_column="variableBorrowIndex",
    )

    # Fix rates
    clean_reserve_data["fixed_variableBorrowRate"] = np.clip(
        clean_reserve_data.variableBorrowRate, a_min=0, a_max=1
    )
    clean_reserve_data["fixed_liquidityRate"] = np.clip(
        clean_reserve_data.liquidityRate, a_min=0, a_max=1
    )
    clean_reserve_data["fixed_utilizationRate"] = np.clip(
        clean_reserve_data.utilizationRate, a_min=0, a_max=1
    )
    return clean_reserve_data


def add_clean_data(
    hourly_reserve_completed: DataFrame, logger: Logger, verbose: bool = False
) -> DataFrame:
    """
    Loop over the assets and call the `add_clean_data_per_asset` function on each asset

    Args:
        hourly_reserve_completed (DataFrame): The dataset to add clean data on.
        logger (Logger): The logger
        verbose (bool): Wether to print details during execution
    Returns:
         DataFrame: The table with extra columns with clean data.
    """
    clean_hourly_reserve = DataFrame()
    assets_list = hourly_reserve_completed.reserve_name.unique().tolist()
    for asset_name in assets_list:
        hourly_asset_reserve = hourly_reserve_completed[
            hourly_reserve_completed.reserve_name == asset_name
        ].copy()
        clean_asset_data = add_clean_data_per_asset(hourly_asset_reserve)
        clean_hourly_reserve = pd.concat((clean_hourly_reserve, clean_asset_data))

    if verbose:
        wrong_borrow_indexes = np.sum(
            (
                clean_hourly_reserve.fixed_variableBorrowIndex
                != clean_hourly_reserve.variableBorrowIndex
            )
        )
        wrong_liquidity_indexes = np.sum(
            (
                clean_hourly_reserve.fixed_liquidityIndex
                != clean_hourly_reserve.liquidityIndex
            )
        )
        logger.log(f"      --> Fixed {wrong_borrow_indexes} borrow index values")
        logger.log(f"      --> Fixed {wrong_liquidity_indexes} liquidity index values")

        wrong_borrow_rates = np.sum(
            (
                clean_hourly_reserve.variableBorrowRate
                != clean_hourly_reserve.fixed_variableBorrowRate
            )
        )
        wrong_liquidity_rates = np.sum(
            (
                clean_hourly_reserve.liquidityRate
                != clean_hourly_reserve.fixed_liquidityRate
            )
        )
        wrong_utilization_rates = np.sum(
            (
                clean_hourly_reserve.utilizationRate
                != clean_hourly_reserve.fixed_utilizationRate
            )
        )
        logger.log(f"      --> Fixed {wrong_borrow_rates} borrow rate values")
        logger.log(f"      --> Fixed {wrong_liquidity_rates} liquidity rate values")
        logger.log(f"      --> Fixed {wrong_utilization_rates} utilization rate values")

    assert len(clean_hourly_reserve) == len(hourly_reserve_completed)

    return clean_hourly_reserve


def remove_indexes_outliers(reserve_data: DataFrame, index_column: str) -> DataFrame:
    """
    Adds a column to reserve_data called `fixed_{index_column}` where the index outliers
    from index_column have been removed.

    Args:
        reserve_data (DataFrame): The dataframe from with the index outliers should be removed
        index_column (str): The name of the index column to process
    Returns:
        DataFrame: A Dataframe similar to reserve_data, but with a extra column named
            `fixed_{index_column}`
    """
    reserve_data_fixed = DataFrame()
    reserve_data["regular_datetime"] = pd.to_datetime(reserve_data.regular_datetime)
    day_min = min(reserve_data.regular_datetime.dt.date)
    day_max = max(reserve_data.regular_datetime.dt.date)
    day = day_min
    assets_list = reserve_data.reserve_name.unique().tolist()
    dict_fill_values = {asset: 1 for asset in assets_list}
    while day <= day_max:
        reserve_data_day = reserve_data[
            reserve_data.regular_datetime.dt.date == day
        ].copy()
        for asset in assets_list:
            reserve_data_day_asset = reserve_data_day[
                reserve_data_day.reserve_name == asset
            ].copy()
            q1 = reserve_data_day_asset[index_column].quantile(0.25)
            q3 = reserve_data_day_asset[index_column].quantile(0.75)
            iqr = q3 - q1
            limit_low = q1 - 1.5 * iqr
            limit_high = q3 + 1.5 * iqr
            reserve_data_day_asset[f"fixed_{index_column}"] = np.where(
                (reserve_data_day_asset[index_column] > limit_high)
                | (reserve_data_day_asset[index_column] < limit_low),
                reserve_data_day_asset[index_column].shift(
                    periods=1, fill_value=dict_fill_values[asset]
                ),
                reserve_data_day_asset[index_column],
            )
            reserve_data_fixed = pd.concat((reserve_data_fixed, reserve_data_day_asset))
            dict_fill_values[asset] = max(
                reserve_data_day_asset[f"fixed_{index_column}"]
            )
        day += Timedelta(days=1)
    assert len(reserve_data_fixed) == len(reserve_data)
    return reserve_data_fixed

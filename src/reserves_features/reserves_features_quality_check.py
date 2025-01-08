"""Data quality check functions for the reserves features extraction"""

import pandas as pd
from pandas import DataFrame
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
    valid_lengths = [28 * 24, 29 * 24, 30 * 24, 31 * 34]
    assert not len(hourly_asset_reserve_completed) in valid_lengths, "Invalid length"
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
    clean_reserve_data = hourly_asset_reserve_completed.sort_values("regular_datetime").reset_index(drop=True)

    # Fix indexes
    n = len(clean_reserve_data)
    clean_reserve_data.loc[0, "fixed_variableBorrowIndex"] = clean_reserve_data.loc[0, "variableBorrowIndex"]
    clean_reserve_data.loc[0, "fixed_liquidityIndex"] = clean_reserve_data.loc[0, "liquidityIndex"]
    clean_reserve_data.loc[n-1, "fixed_variableBorrowIndex"] = clean_reserve_data.loc[n-1, "variableBorrowIndex"]
    clean_reserve_data.loc[n-1, "fixed_liquidityIndex"] = clean_reserve_data.loc[n-1, "liquidityIndex"]

    for index in range(1, n-1):
        prev_liq = clean_reserve_data.loc[index-1, "liquidityIndex"]
        curr_liq = clean_reserve_data.loc[index, "liquidityIndex"]
        next_liq = clean_reserve_data.loc[index+1, "liquidityIndex"]
        prev_bor = clean_reserve_data.loc[index-1, "variableBorrowIndex"]
        curr_bor = clean_reserve_data.loc[index, "variableBorrowIndex"]
        next_bor = clean_reserve_data.loc[index+1, "variableBorrowIndex"]

        if (prev_liq <= curr_liq) & (curr_liq <= next_liq):
            clean_reserve_data.loc[index, "fixed_liquidityIndex"] = clean_reserve_data.loc[index, "liquidityIndex"]
        else:
            clean_reserve_data.loc[index, "fixed_liquidityIndex"] = prev_liq
        
        if (prev_bor <= curr_bor) & (curr_bor <= next_bor):
            clean_reserve_data.loc[index, "fixed_variableBorrowIndex"] = clean_reserve_data.loc[index, "variableBorrowIndex"]
        else:
            clean_reserve_data.loc[index, "fixed_variableBorrowIndex"] = prev_bor
    
    # clean_reserve_data["fixed_variableBorrowIndex"] = (
    #     clean_reserve_data.variableBorrowIndex.cummax()
    # )
    # clean_reserve_data["fixed_liquidityIndex"] = (
    #     clean_reserve_data.liquidityIndex.cummax()
    # )
    
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
        ]
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

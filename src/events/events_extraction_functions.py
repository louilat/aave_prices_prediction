"""Functions for extracting events data"""

from ..utils.utils import run_query
from ..utils.logger import Logger
import pandas as pd
from pandas import DataFrame


def extract_deposit(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        supplies(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            pool \u007b
                pool
            \u007d
            user \u007b
                id
            \u007d
            caller \u007b
                id
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            amount
            assetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["supplies"]


def extract_borrow(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        borrows(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            pool \u007b
                pool
            \u007d
            user \u007b
                id
            \u007d
            caller \u007b
                id
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            amount
            assetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["borrows"]


def extract_redeemUnderlying(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        redeemUnderlyings(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            pool \u007b
                pool
            \u007d
            user \u007b
                id
            \u007d
            to \u007b
                id
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            amount
            assetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["redeemUnderlyings"]


def extract_usageAsCollateral(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        usageAsCollaterals(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            pool \u007b
                pool
            \u007d
            user \u007b
                id
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            fromState
            toState
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["usageAsCollaterals"]


def extract_repay(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        repays(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            pool \u007b
                pool
            \u007d
            user \u007b
                id
            \u007d
            repayer \u007b
                id
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            amount
            useATokens
            assetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["repays"]


def extract_flashloan(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        flashLoans(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            initiator \u007b
                id
            \u007d
            pool \u007b
                pool
            \u007d
            reserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            target
            amount
            totalFee
            protocolFee
            assetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["flashLoans"]


def extract_liquidationCall(
    api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int
) -> dict:
    query = f"""\u007b
        liquidationCalls(
            first: {size},
            skip: {offset},
            orderBy: timestamp,
            orderDirection: desc,
            where: \u007b
                timestamp_gt: {timestamp_min},
                timestamp_lt: {timestamp_max},
            \u007d
        ) \u007b
            id
            txHash
            action
            user \u007b
                id
            \u007d
            pool \u007b
                pool
            \u007d
            collateralReserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            principalReserve \u007b
                underlyingAsset
                name
                decimals
            \u007d
            collateralAmount
            principalAmount
            liquidator
            collateralAssetPriceUSD
            borrowAssetPriceUSD
            timestamp
        \u007d
    \u007d"""
    return run_query(api=api_endpoint, query=query)["data"]["liquidationCalls"]


def fetch_events(
    api_endpoint: str,
    size: int,
    max_queries: int,
    timestamp_min: int,
    timestamp_max: int,
    event: str,
    verbose: bool = False,
) -> DataFrame:
    event_functions = {
        "deposit": extract_deposit,
        "redeemUnderlying": extract_redeemUnderlying,
        "borrow": extract_borrow,
        "usageAsCollateral": extract_usageAsCollateral,
        "repay": extract_repay,
        "flashloan": extract_flashloan,
        "liquidationCall": extract_liquidationCall,
    }

    try:
        extraction_function = event_functions[event]
    except KeyError:
        raise ValueError(f"Unknown event type: {event}")

    events_data = pd.DataFrame()
    for iter in range(max_queries):
        if verbose:
            if iter % 10 == 0:
                print(f"      [Iteration {iter + 1}/{max_queries}]")
        query_output = extraction_function(
            api_endpoint=api_endpoint,
            size=size,
            offset=iter * size,
            timestamp_min=timestamp_min,
            timestamp_max=timestamp_max,
        )
        current_events = pd.json_normalize(query_output)
        if len(current_events) == 0:
            print("All data has been already extracted")
            break
        events_data = pd.concat((events_data, current_events))
    return events_data


def clean_events_data(
    event_name: str, events_data: DataFrame, logger: Logger
) -> DataFrame:
    rename_columns = {
        "pool.pool": "pool",
        "user.id": "user_id",
        "caller.id": "caller_id",
        "to.id": "to_id",
        "reserve.underlyingAsset": "underlying_asset",
        "reserve.name": "reserve_name",
        "reserve.decimals": "reserve_decimals",
        "repayer.id": "repayer_id",
        "initiator.id": "initiator_id",
        "collateralReserve.decimals": "collateral_reserve_decimals",
        "principalReserve.decimals": "principal_reserve_decimals",
        "collateralReserve.name": "collateral_reserve_name",
        "principalReserve.name": "principal_reserve_name",
    }
    clean_data = events_data.rename(columns=rename_columns)

    if event_name not in ["liquidationCall", "usageAsCollateral"]:
        clean_data.reserve_decimals = clean_data.reserve_decimals.astype(int)
        clean_data.amount = (
            clean_data.amount.apply(int) / 10**clean_data.reserve_decimals
        )

    if event_name == "liquidationCall":
        clean_data.collateral_reserve_decimals = (
            clean_data.collateral_reserve_decimals.astype(int)
        )
        clean_data.principal_reserve_decimals = (
            clean_data.principal_reserve_decimals.astype(int)
        )
        clean_data.collateralAmount = (
            clean_data.collateralAmount.apply(int)
            / 10**clean_data.collateral_reserve_decimals
        )
        clean_data.principalAmount = (
            clean_data.principalAmount.apply(int)
            / 10**clean_data.principal_reserve_decimals
        )

    clean_data_ = clean_data.drop_duplicates()

    logger.log(f"      --> Found {len(clean_data) - len(clean_data_)} duplicates")

    return clean_data_

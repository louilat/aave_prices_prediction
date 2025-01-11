"""Functions for extracting events data"""

from ..utils.utils import run_query

def extract_deposits(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_borrows(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_redeemUnderlying(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_usageAsCollateral(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_repay(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_flashloan(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)


def extract_liquidationCall(api_endpoint: str, size: int, offset: int, timestamp_min: int, timestamp_max: int) -> dict:
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
    return run_query(api=api_endpoint, query=query)
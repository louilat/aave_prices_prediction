"""Functions for extracting hourly prices data"""

import pandas as pd
from pandas import DataFrame
from ..utils.utils import run_query


def run_query_prices_v3(
    api_endpoint: str,
    size: int,
    offset: int,
    timestamp_min: int,
    timestamp_max: int,
) -> dict:
    """
    Calls the `run_query` function to query the prices data.

    Args:
        api_endpoint (str): The SubGraph endpoint url
        size (int): The number of items per query
        offset (int): The number of items to skip to start the query
        timestamp_min (int): Used to filter items with a greater timestamp
        timestamp_max (int): Used to filter items with a greater timestamp
    Returns:
        dict: the output of the query
    """
    query = f"""
        \u007b
            marketHourlySnapshots(
                first: {size},
                skip: {offset},
                orderBy: timestamp,
                orderDirection: desc,
                where: \u007b
                    timestamp_lt: {timestamp_max}
                    timestamp_gt: {timestamp_min}
                \u007d
            ) \u007b
                id
                hours
                timestamp
                blockNumber
                market \u007b
                    name
                \u007d
                protocol \u007b
                    protocol
                    name
                \u007d
                inputTokenPriceUSD
                outputTokenPriceUSD
            \u007d
        \u007d
    """
    return run_query(api=api_endpoint, query=query)


def fetch_hourly_prices(
    api_endpoint: str,
    size: int,
    max_queries: int,
    timestamp_min: int,
    timestamp_max: int,
    verbose: bool = False,
) -> DataFrame:
    """
    Calls `run_query_prices_v3` in a loop and returns the concatenated outputs
    in a dataframe.

    Args:
        api_endpoint (str): The SubGraph endpoint url
        size (int): The number of items per query
        max_queries (int): The maximum number of queries
        timestamp_min (int): Used to filter items with a greater timestamp
        timestamp_max (int): Used to filter items with a greater timestamp
        verbose (bool): Wether to print execution details
    Returns:
        DataFrame: The dataframe containing the concatenated outputs of the queries.
    """
    users_balances = pd.DataFrame()
    for iter in range(max_queries):
        if verbose:
            if iter % 10 == 0:
                print(f"      [Iteration {iter + 1}/{max_queries}]")
        query_output = run_query_prices_v3(
            api_endpoint=api_endpoint,
            size=size,
            offset=iter * size,
            timestamp_min=timestamp_min,
            timestamp_max=timestamp_max,
        )
        current_balances = pd.json_normalize(
            query_output["data"]["marketHourlySnapshots"]
        )
        if len(current_balances) == 0:
            print("All data has been already extracted")
            break
        users_balances = pd.concat((users_balances, current_balances))
    return users_balances


def clean_prices_data(prices_raw_data: DataFrame) -> DataFrame:
    """
    Cleans the raw prices data (rename columns, convert timestamp to
    datetime, rename reserves names)

    Args:
        prices_raw_data (DataFrame): output from `fetch_hourly_prices` function
    Returns:
        DataFrame: The clean hourly prices data.
    """
    clean_prices = prices_raw_data.rename(
        columns={
            "market.name": "reserve_name",
            "protocol.name": "protocol_name",
            "protocol.protocol": "protocol",
            "timestamp": "snapshot_timestamp",
            "hours": "timestamp_hours"
        }
    )

    clean_prices["datetime"] = pd.to_datetime(
        clean_prices.timestamp_hours, utc=True, unit="h"
    )

    reserve_names_dict = {
        "Aave Ethereum USDC": "USD Coin",
        "Aave Ethereum WETH": "Wrapped Ether",
        "Aave Ethereum USDT": "Tether USD",
        "Aave Ethereum WBTC": "Wrapped BTC",
        "Aave Ethereum DAI": "Dai Stablecoin",
        "Aave Ethereum LINK": "ChainLink Token",
        "Aave Ethereum rETH": "Rocket Pool ETH",
        "Aave Ethereum GHO": "Gho Token",
        "Aave Ethereum AAVE": "Aave Token",
        "Aave Ethereum MKR": "Maker",
        "Aave Ethereum ENS": "Ethereum Name Service",
        "Aave Ethereum wstETH": "Wrapped liquid staked Ether 2.0",
        "Aave Ethereum CRV": "Curve DAO Token",
        "Aave Ethereum LDO": "Lido DAO Token",
        "Aave Ethereum RPL": "Rocket Pool Protocol",
        "Aave Ethereum LUSD": "LUSD Stablecoin",
        "Aave Ethereum cbETH": "Coinbase Wrapped Staked ETH",
        "Aave Ethereum SNX": "Synthetix Network Token",
        "Aave Ethereum sDAI": "Savings Dai",
        "Aave Ethereum UNI": "Uniswap",
        "Aave Ethereum FRAX": "Frax",
        "Aave Ethereum crvUSD": "Curve.Fi USD Stablecoin",
        "Aave Ethereum FXS": "Frax Share",
        "Aave Ethereum BAL": "Balancer",
        "Aave Ethereum 1INCH": "1INCH Token",
        "Aave Ethereum STG": "StargateToken",
        "Aave Ethereum KNC": "Kyber Network Crystal v2",
    }

    clean_prices.reserve_name = clean_prices.reserve_name.map(reserve_names_dict)

    return clean_prices

"""Functions for processing the users' balances"""

import numpy as np
import pandas as pd
from pandas import DataFrame, NA


def extract_events_data(
    events_inputs_path: str, year: int, month: int
) -> tuple[DataFrame, DataFrame]:
    """
    Fetches the events data corresponfing at `events_inputs_path` to year and month.

    Args:
        events_inputs_path (str):
        year (int): The year of the events snapshot date
        month (int): The month of the events snapshot date
    Returns:
        DataFrame: The concatenated norrow/deposit/repay/redeemUnderlying events dataframes
        DataFrame: The liquidations events dataframe
    """
    events_list = ["borrow", "deposit", "repay", "redeemUnderlying", "liquidationCall"]
    events_data = dict()

    for event_name in events_list:
        month_event_path = (
            events_inputs_path
            + f"{event_name}/"
            + f"events_data_{event_name}_{year}-{month}.csv"
        )
        monthly_event_data = pd.read_csv(month_event_path)
        events_data.update({event_name: monthly_event_data})

    combined_interaction_events = pd.concat(
        (
            events_data["borrow"],
            events_data["deposit"],
            events_data["repay"],
            events_data["redeemUnderlying"],
        )
    )

    assert len(combined_interaction_events.action.unique().tolist()) == 4

    return combined_interaction_events, events_data["liquidationCall"]


def clean_atokens_vtokens_balances(token_balances: DataFrame) -> DataFrame:
    """
    Cleans the balances raw data for either atokens or vtokens raw data.

    Args:
        token_balances (DataFrame): Concatenated outputs from the `users_balances_etl` (atoken_balances)

    Returns:
        DataFrame: The clean atokens or vtokens balances.
    """

    current_balance_column_name = "user_current_atoken_balance"

    clean_tokens_balances = token_balances.copy()

    if "user_current_variable_debt" in clean_tokens_balances.columns.tolist():
        current_balance_column_name = "user_current_vtoken_balance"
        clean_tokens_balances = clean_tokens_balances.rename(
            columns={
                "user_current_variable_debt": "user_current_vtoken_balance",
            }
        )

    clean_tokens_balances["txHash"] = clean_tokens_balances.id.str[126:]

    assert len(clean_tokens_balances.drop_duplicates("id")) == len(
        clean_tokens_balances
    ), "Same transaction appears in several rows"

    clean_tokens_balances = clean_tokens_balances[
        [
            "id",
            "user_address",
            "timestamp",
            "pool",
            "reserve_decimals",
            "reserve_name",
            "usage_as_collateral_enabled",
            current_balance_column_name,
        ]
    ]

    return clean_tokens_balances


def clean_events(events: DataFrame) -> DataFrame:
    """
    Processes the Supply/RedeemUnderlying/Borrow/Repay events, in order to
    get only one event per (txHash, reserve, user_address)
        1. Creates a column `a_amount` equal to:
            amount if action is Supply
            -amount if action is RedeemUnderlying
            0 else
        2. Creates a column `v_amount` equal to:
            amount if action is Borrow
            -amount if action is Repay
            0 else
        3. Combine events having shared caracteristics (txHash, reserve_name,
            user_address, pool, timestamp)

    Args:
        events: Concatenated Supply/Borrow/redeem/Repay events table coming from `events_etl`
    Returns:
        DataFrame: The events dataframe ready to be merge with the balances dataframe.
    """
    tx_hash_list = events.txHash.unique()
    clean_events = events.copy()
    clean_events["a_amount"] = np.select(
        condlist=[
            clean_events.action == "Supply",
            clean_events.action == "RedeemUnderlying",
        ],
        choicelist=[clean_events.amount, -clean_events.amount],
        default=0,
    )
    clean_events["v_amount"] = np.select(
        condlist=[clean_events.action == "Borrow", clean_events.action == "Repay"],
        choicelist=[clean_events.amount, -clean_events.amount],
        default=0,
    )
    tx_count = events.groupby(
        ["txHash", "reserve_name", "timestamp", "pool", "user_id"]
    )["action"].transform("count")
    multiple_events = clean_events[tx_count > 1]

    multiple_events = multiple_events.groupby(
        ["txHash", "reserve_name", "timestamp", "pool", "user_id"], as_index=False
    ).aggregate({"a_amount": "sum", "v_amount": "sum"})
    multiple_events["action"] = "Multiple"

    clean_events = pd.concat((clean_events[tx_count == 1], multiple_events))

    assert len(
        clean_events.drop_duplicates(["txHash", "reserve_name", "user_id"])
    ) == len(
        clean_events
    ), "Should be only one transaction per txHash, reserve, user: it is not the case"
    assert len(tx_hash_list) == len(
        clean_events.txHash.unique()
    ), "Some transaction were dropped during cleaning"

    clean_events = clean_events.rename(columns={"user_id": "user_address"})
    clean_events = clean_events[
        [
            "txHash",
            "user_address",
            "reserve_name",
            "timestamp",
            "pool",
            "action",
            "a_amount",
            "v_amount",
        ]
    ].copy()

    return clean_events


def clean_liquidation_events(liquidations: DataFrame) -> DataFrame:
    """
    Processes the liquidations events in order to get one row per (user_address, txHash, reserve_name, action).
    Action can be either `trigger_liquidation` or `is_liquidated`
    WARNING: This function does not compute any liquidation amount (TODO)

    Args:
        liquidations (DataFrame): Output from the `events_etl` (liquidation table)
    Returns:
        DataFrame: The liquidations events dataframe ready to be merge with the balances dataframe
    """
    liquidators_collateral_data = liquidations[
        ["liquidator", "txHash", "collateral_reserve_name", "timestamp", "pool"]
    ]
    liquidators_collateral_data = liquidators_collateral_data.rename(
        columns={
            "collateral_reserve_name": "reserve_name",
            "liquidator": "user_address",
        }
    )
    liquidators_collateral_data["action"] = "trigger_liquidation"

    liquidators_debt_data = liquidations[
        ["liquidator", "txHash", "principal_reserve_name", "timestamp", "pool"]
    ]
    liquidators_debt_data = liquidators_debt_data.rename(
        columns={"principal_reserve_name": "reserve_name", "liquidator": "user_address"}
    )
    liquidators_debt_data["action"] = "trigger_liquidation"

    liquidated_collateral_data = liquidations[
        ["user_id", "txHash", "collateral_reserve_name", "timestamp", "pool"]
    ]
    liquidated_collateral_data = liquidated_collateral_data.rename(
        columns={"collateral_reserve_name": "reserve_name", "user_id": "user_address"}
    )
    liquidated_collateral_data["action"] = "is_liquidated"

    liquidated_debt_data = liquidations[
        ["user_id", "txHash", "principal_reserve_name", "timestamp", "pool"]
    ]
    liquidated_debt_data = liquidated_debt_data.rename(
        columns={"principal_reserve_name": "reserve_name", "user_id": "user_address"}
    )
    liquidated_debt_data["action"] = "is_liquidated"

    clean_liquidations = pd.concat(
        (
            liquidators_collateral_data,
            liquidators_debt_data,
            liquidated_collateral_data,
            liquidated_debt_data,
        )
    )
    assert len(clean_liquidations) == 4 * len(liquidations)

    return clean_liquidations.drop_duplicates()


def match_balances_with_events(
    combined_atokens_vtokens_balances: DataFrame,
    clean_interaction_events: DataFrame,
    clean_liquidation_events: DataFrame,
):
    """
    Merges the users's balances with the events.

    Args:
        combined_atokens_vtokens_balances (DataFrame): Output from combine_atokens_vtokens_balances() function
        clean_interaction_events (DataFrame): Output from clean_events() function
        clean_liquidation_events (DataFrame): Output from clean_liquidation_events() function
    Returns:
        DataFrame: The users' balances dataframe matched with the events.
    """
    combined_atokens_vtokens_balances["txHash"] = (
        combined_atokens_vtokens_balances.id.str[126:]
    )
    all_clean_events = pd.concat((clean_interaction_events, clean_liquidation_events))

    all_clean_events = all_clean_events.drop_duplicates(
        ["txHash", "user_address", "reserve_name", "timestamp", "pool"]
    )  # To avoid duplicates when an event appears both in interaction events and liquidation events

    balances_with_events = combined_atokens_vtokens_balances.merge(
        all_clean_events,
        how="left",
        on=["txHash", "user_address", "reserve_name", "timestamp", "pool"],
    )

    assert len(combined_atokens_vtokens_balances) == len(
        balances_with_events
    ), "Multiple events found for 1 balance snapshot"

    return balances_with_events

import pytest
import pandas as pd
from pandas import DataFrame, Timestamp
from ...src.reserves_features.reserves_features_quality_check import (
    add_clean_data_per_asset,
)


@pytest.fixture
def hourly_asset_reserve_completed():
    regular_datetime = [
        Timestamp("2023-01-27 08:00:00"),
        Timestamp("2023-01-27 09:00:00"),
        Timestamp("2023-01-27 10:00:00"),
        Timestamp("2023-01-27 11:00:00"),
        Timestamp("2023-01-27 12:00:00"),
        Timestamp("2023-01-27 13:00:00"),
        Timestamp("2023-01-27 14:00:00"),
        Timestamp("2023-01-27 15:00:00"),
        Timestamp("2023-01-27 16:00:00"),
        Timestamp("2023-01-27 17:00:00"),
        Timestamp("2023-01-27 18:00:00"),
        Timestamp("2023-01-27 19:00:00"),
        Timestamp("2023-01-27 20:00:00"),
        Timestamp("2023-01-27 21:00:00"),
        Timestamp("2023-01-27 22:00:00"),
    ]
    availableLiquidity = [
        0.5847671558844771,
        0.5847671558844771,
        90.58476715588448,
        90.58476715588448,
        90.58476715588448,
        1591.0487893901566,
        0.0,
        17421.773131785023,
        168709.87034232458,
        168559.87034232458,
        137559.87034232458,
        135959.87034232458,
        135534.87034232458,
        125320.6483109768,
        125320.6483109768,
    ]
    liquidityIndex = [
        1.0,
        1.0,
        1.0000035930796474,
        1.0000035930796474,
        1.0000035930796474,
        1.0000000000000006,
        1.000023207403959,
        1.0000933818439846,
        1.0000969690200598,
        1.000098300973917,
        1.0000983684214773,
        1.00009837975782,
        1.000098724297148,
        1.000099496278401,
        1.000099496278401,
    ]
    liquidityRate = [
        0.0,
        0.0,
        5.365021392540512e-06,
        5.365021392540512e-06,
        5.365021392540512e-06,
        1.775437625943373e-08,
        0.7110000000000001,
        0.3625468649395837,
        0.3625464734361676,
        0.0005063850466211,
        0.0021277841768364,
        0.003292219215388,
        0.0035020255128203,
        0.0045301878994459,
        0.0045301878994459,
    ]
    totalATokenSupply = [
        1.5847671558844771,
        1.5847671558844771,
        91.58477284629848,
        91.58477284629848,
        91.58477284629848,
        1592.0487952456624,
        171275.78614340903,
        188709.91136304167,
        188710.68778610893,
        188560.93894879788,
        188560.95165707995,
        188560.95379304216,
        188561.01871018577,
        183560.9153043215,
        183560.9153043215,
    ]
    totalCurrentVariableDebt = [
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        171275.87463183305,
        171275.87463183305,
        20002.735051876836,
        20002.735051876836,
        51002.770475477664,
        52602.842654256136,
        53027.865548230926,
        58241.96043716357,
        58241.96043716357,
    ]
    totalLiquidity = [
        1.5847671558844771,
        1.5847671558844771,
        91.58476715588448,
        91.58476715588448,
        91.58476715588448,
        1592.0487893901566,
        171274.0,
        188695.773131785,
        188695.87313178505,
        188545.87313178505,
        188545.87313178505,
        188545.87313178505,
        188545.87313178505,
        183545.6242713563,
        183545.6242713563,
    ]
    utilizationRate = [
        0.63100752,
        0.63100752,
        0.01091884,
        0.01091884,
        0.01091884,
        0.00062812,
        1.0,
        1.5,
        0.10591648,
        0.10600074,
        0.27041696,
        0.27890296,
        0.28115705,
        0.31722344,
        0.31722344,
    ]
    variableBorrowIndex = [
        1.0,
        1.0,
        1.0000063269029156,
        1.0000063269029156,
        1.0000063269029156,
        1.0000065104599751,
        1.000030181217611,
        1.000110369381822,
        1.0001147606055416,
        1.0001172265724676,
        1.0001179330493064,
        1.0001179909763598,
        1.0001194063366283,
        1.0001224311111103,
        1.0001224311111103,
    ]
    variableBorrowRate = [
        0.0,
        0.0,
        0.0005459457335944,
        0.0005459457335944,
        0.0005459457335944,
        3.1406277026021525e-05,
        0.7899999999999999,
        0.4438014287468094,
        0.4438010047777893,
        0.0053040081627072,
        0.0108724528992528,
        0.0135240921145029,
        0.013948368110086,
        0.0158643343864397,
        0.0158643343864397,
    ]
    reserve_name = [
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
        "Dai Stablecoin",
    ]
    true_value = [
        1.0,
        0.0,
        1.0,
        0.0,
        0.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        1.0,
        0.0,
    ]
    return DataFrame(
        {
            "regular_datetime": regular_datetime,
            "availableLiquidity": availableLiquidity,
            "liquidityIndex": liquidityIndex,
            "liquidityRate": liquidityRate,
            "totalATokenSupply": totalATokenSupply,
            "totalCurrentVariableDebt": totalCurrentVariableDebt,
            "totalLiquidity": totalLiquidity,
            "utilizationRate": utilizationRate,
            "variableBorrowIndex": variableBorrowIndex,
            "variableBorrowRate": variableBorrowRate,
            "reserve_name": reserve_name,
            "true_value": true_value,
        }
    )


def test_add_clean_data_per_asset(hourly_asset_reserve_completed):
    clean_data = add_clean_data_per_asset(hourly_asset_reserve_completed)
    assert (
        clean_data.columns.tolist()
        == hourly_asset_reserve_completed.columns.tolist()
        + [
            "fixed_variableBorrowIndex",
            "fixed_liquidityIndex",
            "fixed_variableBorrowRate",
            "fixed_liquidityRate",
            "fixed_utilizationRate",
        ]
    )
    assert (
        clean_data.fixed_variableBorrowIndex.tolist()
        == hourly_asset_reserve_completed.variableBorrowIndex.tolist()
    )
    assert clean_data.fixed_liquidityIndex.tolist()[5] == 1.0000035930796474
    assert clean_data.fixed_utilizationRate.tolist()[7] == 1.0
    assert (
        clean_data.fixed_variableBorrowRate.tolist()
        == hourly_asset_reserve_completed.variableBorrowRate.tolist()
    )

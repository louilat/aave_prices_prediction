import pytest
from ...src.reserves_features.reserves_features import fill_missing_data
from pandas import Timestamp, DataFrame
import numpy as np


@pytest.fixture
def reserves_history_hourly_selected_assets():
    accruedToTreasury = [
        148.298653301566,
        148.298653301566,
        425.62500421230493,
        425.62500421230493,
        644.7576589563412,
        807.1469663311733,
        807.1469663311733,
        807.1469663311733,
        1200.6099587959015,
        1200.6099587959015,
        1200.6099587959015,
        1721.7655661542403,
        1721.7655661542403,
    ]
    availableLiquidity = [
        9044475.841795122,
        8900327.47283397,
        8314924.234420961,
        8920361.686003985,
        8896905.290757544,
        8891928.723719828,
        5833802.379529142,
        6492105.048612419,
        6537660.16105298,
        6666456.88938374,
        5857508.97453999,
        6677449.3511001,
        6508006.608639125,
    ]
    averageStableBorrowRate = [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ]
    liquidityIndex = [
        1.0339472968377017,
        1.033967344855965,
        1.0339716134347943,
        1.0339832662659894,
        1.0339918298814152,
        1.0340068155305207,
        1.034013280912986,
        1.0340336812148072,
        1.0340473070286633,
        1.0340596760906633,
        1.0340600772643511,
        1.0340935744795816,
        1.034101320181211,
    ]
    liquidityRate = [
        0.03916690890104191,
        0.03916712805858124,
        0.039167174719763484,
        0.03928793853327737,
        0.039288031174983404,
        0.039308116540360344,
        0.12433682746032954,
        0.1270897035777757,
        0.08211692177738038,
        0.07282561535500456,
        0.07300935283225926,
        0.07297550971802418,
        0.07237497523209199,
    ]
    priceInEth = [
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
        100011000,
    ]
    priceInUsd = [
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
        "100011000",
    ]
    stableBorrowRate = [
        0.06491702188930479,
        0.06491703564582707,
        0.06491703857474528,
        0.06492461307205251,
        0.06492461887821473,
        0.06492587753292327,
        0.16623410083303047,
        0.16950864789175984,
        0.1156242451878428,
        0.10438659181522822,
        0.10460917454325913,
        0.10456817749702434,
        0.10384061765019383,
    ]
    timestamp = [
        1704508811,
        1704512735,
        1704515807,
        1704524519,
        1704532283,
        1704543851,
        1704547811,
        1704554279,
        1704557183,
        1704562187,
        1704563423,
        1704576191,
        1704579611,
    ]
    totalATokenSupply = [
        104310654.3721639,
        104312675.59070648,
        104313105.94399482,
        104266813.38924862,
        104267676.37038755,
        104269186.51905808,
        101211711.7107851,
        101213707.21570751,
        101220595.18770978,
        101221805.16383766,
        101221844.40776595,
        101226113.66043517,
        101226871.3739694,
    ]
    totalCurrentVariableDebt = [
        92348685.2805688,
        92493398.51852489,
        93079243.9812435,
        92427449.90263042,
        92452170.60079625,
        92458817.70243607,
        92458817.70243607,
        91803969.2834209,
        91764833.43647757,
        91637380.01694538,
        92446649.77011944,
        91631026.62491493,
        91801352.10868306,
    ]
    totalLiquidity = [
        99323640.50546567,
        99323640.50546567,
        99323640.50546567,
        99276173.12546566,
        99276173.12546566,
        99276173.12546566,
        96218046.78127497,
        96218046.78127497,
        96223601.89371555,
        96223601.89371555,
        96223601.89371555,
        96224594.3554319,
        96224594.3554319,
    ]
    totalPrincipalStableDebt = [
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ]
    totalScaledVariableDebt = [
        88164719.12889692,
        88302335.70753793,
        88861209.69929703,
        88237891.14215572,
        88260283.99775405,
        88265034.83533219,
        88265034.83533219,
        87636615.49051799,
        87598431.60236675,
        87475483.20045996,
        88247688.39878234,
        87465937.75052097,
        87627676.91432579,
    ]
    utilizationRate = [
        0.90893934,
        0.91039064,
        0.91628454,
        0.91014599,
        0.91038226,
        0.91043239,
        0.93936893,
        0.93252715,
        0.93205762,
        0.9307191,
        0.93912606,
        0.93060558,
        0.93236649,
    ]
    variableBorrowIndex = [
        1.0474361531207925,
        1.047461650005826,
        1.0474670787210487,
        1.047481896913689,
        1.0474927713135258,
        1.0475117963555598,
        1.0475200039097856,
        1.0475451417497899,
        1.0475619949008266,
        1.0475773671427318,
        1.0475778659827233,
        1.0476195174775544,
        1.0476291497616133,
    ]
    variableBorrowRate = [
        0.049170218893047866,
        0.04917035645827077,
        0.04917038574745272,
        0.04924613072052512,
        0.04924618878214716,
        0.0492587753292327,
        0.15123410083303046,
        0.15450864789175983,
        0.10062424518784278,
        0.08938659181522822,
        0.08960917454325913,
        0.08956817749702434,
        0.08884061765019383,
    ]
    reserve_decimals = [18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18]
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
    ]
    reserve_pool = [
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
    ]
    datetime = [
        Timestamp("2024-01-06 03:00:00"),
        Timestamp("2024-01-06 04:00:00"),
        Timestamp("2024-01-06 05:00:00"),
        Timestamp("2024-01-06 07:00:00"),
        Timestamp("2024-01-06 09:00:00"),
        Timestamp("2024-01-06 12:00:00"),
        Timestamp("2024-01-06 14:00:00"),
        Timestamp("2024-01-06 15:00:00"),
        Timestamp("2024-01-06 16:00:00"),
        Timestamp("2024-01-06 17:00:00"),
        Timestamp("2024-01-06 18:00:00"),
        Timestamp("2024-01-06 21:00:00"),
        Timestamp("2024-01-06 22:00:00"),
    ]

    output = DataFrame(
        {
            "accruedToTreasury": accruedToTreasury,
            "availableLiquidity": availableLiquidity,
            "averageStableBorrowRate": averageStableBorrowRate,
            "liquidityIndex": liquidityIndex,
            "liquidityRate": liquidityRate,
            "priceInEth": priceInEth,
            "priceInUsd": priceInUsd,
            "stableBorrowRate": stableBorrowRate,
            "timestamp": timestamp,
            "totalATokenSupply": totalATokenSupply,
            "totalCurrentVariableDebt": totalCurrentVariableDebt,
            "totalLiquidity": totalLiquidity,
            "totalPrincipalStableDebt": totalPrincipalStableDebt,
            "totalScaledVariableDebt": totalScaledVariableDebt,
            "utilizationRate": utilizationRate,
            "variableBorrowIndex": variableBorrowIndex,
            "variableBorrowRate": variableBorrowRate,
            "reserve_decimals": reserve_decimals,
            "reserve_name": reserve_name,
            "reserve_pool": reserve_pool,
            "datetime": datetime,
        }
    )
    return output


def test_fill_missing_data(reserves_history_hourly_selected_assets):
    completed_reserves_history = fill_missing_data(
        reserves_history_hourly_selected_assets
    )
    # Output shape
    assert completed_reserves_history.columns.tolist() == [
        "regular_datetime"
    ] + reserves_history_hourly_selected_assets.columns.tolist()[:-1] + ["true_value"]
    assert len(completed_reserves_history) == 20
    completed_reserves_history = completed_reserves_history.set_index(
        "regular_datetime"
    )

    # Existing value
    assert (
        completed_reserves_history.loc[Timestamp("2024-01-06 03:00:00"), "reserve_name"]
        == "Dai Stablecoin"
    )
    assert (
        np.round(
            completed_reserves_history.loc[
                Timestamp("2024-01-06 03:00:00"), "variableBorrowRate"
            ],
            2,
        )
        == 0.05
    )
    assert (
        np.round(
            completed_reserves_history.loc[
                Timestamp("2024-01-06 03:00:00"), "availableLiquidity"
            ],
            2,
        )
        == 9044475.84
    )

    # Missing value
    assert (
        completed_reserves_history.loc[Timestamp("2024-01-06 20:00:00"), "reserve_name"]
        == "Dai Stablecoin"
    )

    assert (
        np.round(
            completed_reserves_history.loc[
                Timestamp("2024-01-06 20:00:00"), "variableBorrowRate"
            ],
            2,
        )
        == 0.09
    )

    assert (
        np.round(
            completed_reserves_history.loc[
                Timestamp("2024-01-06 20:00:00"), "totalScaledVariableDebt"
            ],
            2,
        )
        == 88247688.40
    )

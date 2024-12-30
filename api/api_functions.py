"""Functions for running the API"""

import pandas as pd
from pandas import DataFrame
from datetime import datetime


def get_balance_sheet(
    asset: str, year: int, month: int, day: int, hour: int
) -> DataFrame:
    reserve_data = pd.read_csv(
        f"https://minio.lab.sspcloud.fr/llatournerie/aave-data/data-prod/aave-v3/reserves-features/reserves_history_hourly_selected_assets_completed_{year}-{month}.csv",
        usecols=[
            "regular_datetime",
            "accruedToTreasury",
            "availableLiquidity",
            "totalCurrentVariableDebt",
            "reserve_name",
        ],
    )
    reserve_data.regular_datetime = pd.to_datetime(reserve_data.regular_datetime)
    snapshot_datetime = datetime(year, month, day, hour)
    reserve_data = reserve_data[
        (reserve_data.reserve_name == asset)
        & (reserve_data.regular_datetime == snapshot_datetime)
    ]
    return str(reserve_data.to_dict(orient="list"))

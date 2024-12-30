"""Aave Data API"""

from fastapi import FastAPI
from api_functions import get_balance_sheet

app = FastAPI(
    title="AAVE V3 Data Provider",
    description="An application to access the Aave V3 data 🚢 <br>An API version to facilitate data access 🚀"
    + '<br><br><img src="https://cryptologos.cc/logos/aave-aave-logo.png" width="200">',
)


@app.get("/", tags=["Welcome"])
def show_welcome_page():
    """
    Show welcome page with model name and version.
    """

    return {
        "Message": "API de prédiction de survie sur le Titanic",
        "Model_name": "Titanic ML",
        "Model_version": "0.1",
    }


@app.get("/balance_sheets", tags=["Balance Sheet"])
async def predict(
    asset: str = "Dai Stablecoin",
    year: int = 2024,
    month: int = 7,
    day: int = 18,
    hour: int = 14,
) -> str:
    """ """

    balance_info = get_balance_sheet(asset, year, month, day, hour)

    return balance_info

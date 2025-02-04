# Aave Data Extraction [![Build](https://github.com/louilat/aave_prices_prediction/actions/workflows/test.yaml/badge.svg)](https://github.com/louilat/aave_prices_prediction/actions/workflows/test.yaml)

Repository for Aave V2 and Aave V3 data extraction using TheGraph protocol.

## About this repo

This repo provides the following data about Aave V2 and Aave V3:

- Reserves information: Hourly lending and borrowing rates and index, available liquidity in the reserve, total amount deposited, total amount borrowed
- Hourly asset prices
- Users balances: Users' aTokens and vTokens balances at tick level. 
- Pool contract events: Collects the following events: Borrow, Deposit, RedeemUnderlying, Repay, UsageAsCollateral, FlashLoan, LiquidationCall. 

## Using this repo

To clone this repository:


## Collected Data

### Reserves Info

The reserves features dataset contains information about the overall state of the reserves: amount borrowed, amount deposited, rates, indexes, etc. These datasets have the form of a panel with hourly granularity, which means that each hour and reserve pair has its corresponding row. As the reserve data is updated at tick level (i.e. when a user interacts with the reserve), if a reserve has not been used for a given hour, the reserve state is left unchanged and filled with the last available data.

The following table provides details about the reserve features datasets.

### Hourly Asset Prices

The prices dataset contains the hourly prices data corresponding to the reserves' underlying assets. This data comes from the Messari subgraph. 

The following table provides details about the hourly prices datasets.

Name                       | Type      | Example             | Description                                                                                           |
---                        |---        |---                  |---                                                                                                    |
regular_datetime           | Datetime  | 2023-03-01 00:00:00 | Datetime of the panel data (regular step of 1 hour)                                                   |
availableLiquidity         | Float     | 96785.73789690784   | Amount of underlying asset (UA) deposited in the reserve that is not borrowed                         |
liquidityIndex             | Float     | 1.0164497185622012  | Amount of UA that a despositor could claim if he had deposited one unit at the opening of the reserve |
liquidityRate              | Float     | 0.0179316703081626  | Current rate earned by the depositors                                                                 |
priceInEth                 | Float     |                     | Price of the UA in eth (INCORRECT FOR V3!)                                                            |
priceInUsd                 | Float     |                     | Price of the UA in usd (INCORRECT FOR V3!)                                                            |
stableBorrowRate           | Float     | 0.0968183339558269  | IRRELEVANT - STABLE RATE BORROWING HAS BEEN DEPRECATED                                                |
timestamp                  | Int       | 1701392375          | Datetime of the latest user interaction with the reserve (number of sec since unix time)              |
totalATokenSupply          | Float     | 432758.7886421056   | Total amount of UA units deposited in the reserve                                                     |
totalCurrentVariableDebt   | Float     | 335978.2235957373   | Total amount of UA units borrowed from the reserve                                                    |
totalLiquidity             | Float     | 427500.3680213676   | Total                                                                                                 |
totalPrincipalStableDebt   | Float     | 0.0                 | IRRELEVANT - STABLE RATE BORROWING HAS BEEN DEPRECATED                                                |
totalScaledVariableDebt    | Float     | 326065.7378284734   | Total amount of debt, rescaled by the current variable borrow index                                   |
utilizationRate            | Float     | 0.7736008           | = totalCurrentVariableDebt / (totalCurrentVariableDebt + availableLiquidity)                          |
variableBorrowIndex        | Float     | 1.0303995114130782  | Amount of UA units that a borrower would have to repay if he had borrowed one unit at the opening of the reserve           |
variableBorrowRate         | Float     | 0.0271728337690788  | Current rate payed by the borrowers                                                                   |
reserve_decimals           | Int       | 18                  | Number of decimals used to store reserves data as integers                                            |
reserve_name               | String    | Wrapped Ether       | Name of the reserve                                                                                   |
reserve_pool               | String    | 0x87870bca...       | Address of the mainnet pool contract                                                                  |
true_value                 | Bool      | True                | True if the latest user interaction with the reserve occured during the regular_datetime hour, else False            |



### Users Balances

The following table provides details about the users' balances datasets.

### Events

The following table provides details about the events datasets.
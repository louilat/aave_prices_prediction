# Aave Data Extraction [![Build](https://github.com/louilat/aave_prices_prediction/actions/workflows/test.yaml/badge.svg)](https://github.com/louilat/aave_prices_prediction/actions/workflows/test.yaml)

Repository for Aave V2 and Aave V3 data extraction using TheGraph protocol. The data is mainly collected from [`Aave protocol-V3 Subgraph`](https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g?view=Query&chain=arbitrum-one) and [`Aave protocol-V2 Subgraph`](https://thegraph.com/explorer/subgraphs/8wR23o1zkS4gpLqLNU4kG3JHYVucqGyopL5utGxP2q1N?view=Query&chain=arbitrum-one), except for the hourly prices data for V3 that is collected from [`Aave v3 Messari Subgraph`](https://thegraph.com/explorer/subgraphs/JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk?view=Query&chain=arbitrum-one). 

## About this repo

This repo provides the following data about Aave V2 and Aave V3:

- Reserves features: Hourly lending and borrowing rates and index, available liquidity in the reserve, total amount deposited, total amount borrowed
- Hourly asset prices (for V3 only because the prices given in the reserves features datasets is incorrect)
- Users balances: Users' aTokens and vTokens balances at tick level. 
- Pool contract events: Collects the following events: Borrow, Deposit, RedeemUnderlying, Repay, UsageAsCollateral, FlashLoan, LiquidationCall. 

## Using this repo

To clone this repository:

```bash
git clone https://github.com/louilat/aave_prices_prediction.git
```



## Collected Data

### Reserves Info

The reserves features dataset contains information about the overall state of the reserves: amount borrowed, amount deposited, rates, indexes, etc. These datasets have the form of a panel with hourly granularity, which means that each hour and reserve pair has its corresponding row. As the reserve data is updated at tick level (i.e. when a user interacts with the reserve), if a reserve has not been used for a given hour, the reserve state is left unchanged and filled with the last available data.

The following table provides details about the reserve features datasets.

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


### Hourly Asset Prices

The prices dataset is available for V3 only (as for V2 the columns `priceInEth` and `priceInUsd` from the `reserves_features` dataset are already available and consistent) contains the hourly prices data corresponding to the reserves' underlying assets. This data comes from the [`Messari Subgraph`](https://thegraph.com/explorer/subgraphs/JCNWRypm7FYwV8fx5HhzZPSFaMxgkPuw4TnR3Gpi81zk?view=Query&chain=arbitrum-one). 

The following table provides details about the hourly prices datasets.

Name                  | Type     | Example              | Description                                                              |
---                   |---       |---                   |---                                                                       |
blockNumber           | Int      | 16530243             | Number of the block from which the asset price has been extracted        |
timestamp_hours       | Int      | 465335               | Time corresponding to the price (in number of hours since unix time)     |
id                    | String   | 0x98c23e...          | = Smart contract address of the reserve + timestamp_hours                |
inputTokenPriceUSD    | Float    | 1                    | USD Price of the underlying asset                                        |
outputTokenPriceUSD   | Float    | 1                    | USD Price of the reserve token (MOSTLY IRRELEVANT)                       |
snapshot_timestamp    | Int      | 1675209551           | Timestamp of the latest available price used for the corresponding hour  |
reserve_name          | String   | USD Coin             | Name of the reserve                                                      |
protocol_name         | String   | Aave v3              | Full name of the protocol                                                |
protocol              | String   | Aave                 | Name of the protocol                                                     |
datetime              | Datetime | 2023-01-01 00:00:00  | Datetime corresponding to timestamp_hours                                |



### Users Balances

The following table provides details about the users' balances datasets.

Name                             | Type     | Example          | Description                                                               |
---                              |---       |---               |---                                                                        |
id                               | String   | 0x55e1602c...    | = userReserve + txHash                                                    |
user_address                     | String   | 0x55e160...      | Address of the user                                                       |
timestamp                        | Int      | 1675204919       | Datetime in seconds since unix timestamp                                  |
pool                             | String   | 0x87870b         | Address of the smart contract of the pool                                 |
reserve_decimals                 | Int      | 18               | Number of decimals used to store reserves data as integers                |
reserve_name                     | String   | Wrapped Ether    | Name of the reserve                                                       |
usage_as_collateral_enabled      | Bool     | True             | Whether the user has enable the use as collateral of his balance          |
user_current_[a/v]token_balance  | Float    | 3,5              | Balance of the the user in the corresponding reserve                      |
txHash                           | String   | 0x359d5468e...   | Hash of the transaction corresponding to the user balance's snapshot      |
action                           | String   | Supply           | Name of the event corresponding to the snapshot                           |
a_amount                         | Float    | 3,5              | Variation of atokens in the user balance implied by the event             |
v_amount                         | Float    | 0                | Variation of vtokens in the user balance implied by the event             |

### Events

The following table provides details about the events datasets.

#### For Borrow/Deposit events:

Name                  | Type     | Example              | Description                                                 |
---                   |---       |---                   |---                                                          |
action                | String   | Borrow               | Name of the event (here Borrow or Deposit)                  |
amount                | Float    | 30000                | Amount borrowed/deposited by the user                       |
assetPriceUSD         | Float    | 1                    | Price in USD of the corresponding asset (INCORRECT FOR V3!) |
id                    | String   | 16530243:5:0xc855... | Unique id corresponding to the event                        |
timestamp             | Int      | 1675209551           | Datetime of the event (in seconds since unix timestamp)     |
txHash                | String   | 0xc855473a4...       | Hash of the transaction corresponding to the event          |
caller_id             | String   | 0xe98594...          | Address of the user who triggered this action.              |
pool                  | String   | 0x87870bca...        | Address of the pool smart contract                          |
reserve_decimals      | Int      | 6                    | Number of decimals used to store reserves data as integers  |
reserve_name          | String   | USD Coin             | Name of the reserve from which the user borrowed/deposited  |
underlying_asset      | String   | 0xa0b86991c...       | Address of the underlying asset smart contract              |
user_id               | String   | 0xe985948...         | Address of the user receiving the vtokens/atokens           |

#### For Repay Events:

Name                  | Type     | Example              | Description                                                 |
---                   |---       |---                   |---                                                          |
action                | String   | Repay                | Name of the event (here Borrow or Deposit)                  |
amount                | Float    | 30000                | Amount borrowed/deposited by the user                       |
assetPriceUSD         | Float    | 1                    | Price in USD of the corresponding asset (INCORRECT FOR V3!) |
id                    | String   | 16530243:5:0xc855... | Unique id corresponding to the event                        |
timestamp             | Int      | 1675209551           | Datetime of the event (in seconds since unix timestamp)     |
txHash                | String   | 0xc855473a4...       | Hash of the transaction corresponding to the event          |
repayer_id            | String   | 0xe98594...          | Address of the user who repaid the debt.                    |
pool                  | String   | 0x87870bca...        | Address of the pool smart contract                          |
reserve_decimals      | Int      | 6                    | Number of decimals used to store reserves data as integers  |
reserve_name          | String   | USD Coin             | Name of the reserve from which the user borrowed/deposited  |
underlying_asset      | String   | 0xa0b86991c...       | Address of the underlying asset smart contract              |
user_id               | String   | 0xe985948...         | Address of the user getting his debt reduced                |

#### For RedeemUnderlying Events:

Name                  | Type     | Example              | Description                                                 |
---                   |---       |---                   |---                                                          |
action                | String   | RedeemUnderlying     | Name of the event (here Borrow or Deposit)                  |
amount                | Float    | 30000                | Amount borrowed/deposited by the user                       |
assetPriceUSD         | Float    | 1                    | Price in USD of the corresponding asset (INCORRECT FOR V3)  |
id                    | String   | 16530243:5:0xc855... | Unique id corresponding to the event                        |
timestamp             | Int      | 1675209551           | Datetime of the event (in seconds since unix timestamp)     |
txHash                | String   | 0xc855473a4...       | Hash of the transaction corresponding to the event          |
user_id               | String   | 0xe98594...          | Address of the user who redeemed his atokens.               |
pool                  | String   | 0x87870bca...        | Address of the pool smart contract                          |
reserve_decimals      | Int      | 6                    | Number of decimals used to store reserves data as integers  |
reserve_name          | String   | USD Coin             | Name of the reserve from which the user borrowed/deposited  |
underlying_asset      | String   | 0xa0b86991c...       | Address of the underlying asset smart contract              |
to_id                 | String   | 0xe985948...         | Address of the user receiving the underlying asset          |

#### For LiquidationCall Events:

Name                                | Type     | Example                | Description                                                 |
---                                 |---       |---                     |---                                                          |
action                              | String   | LiquidationCall        | Name of the event                                           |
borrowAssetPriceUSD                 | Float    | 1                      | USD price of the liquidated debt account                    |
collateralAmount                    | Float    | 0,857                  | Amount of collateral seized by the liquidator               |
collateralAssetPriceUSD             | Float    | 1542,475               | USD price of the liquidated collateral account              |
id                                  | String   | 16521648:19:0xf89d...  | Unique id of the event                                      |
liquidator                          | String   | 0x3697e949...          | Address of the liquidator                                   |
principalAmount                     | Float    | 1265,884               | Amount of debt repaid by the liquidator                     |
timestamp                           | Int      | 1675105907             | Datetime of the liquidation (in sec since unix time)        |
txHash                              | String   | 0xf89d68692625...      | Transaction hash of the liquidation event                   |
collateral_reserve_decimals         | Int      | 18                     | Number of decimals used to store reserves data as integers  |
collateral_reserve_name             | String   | Wrapped Ether          | Name of the liquidation's collateral reserve                |
collateralReserve.underlyingAsset   | String   | 0xc02aaa39b...         | Contract address of the collateral underlying asset         |
pool                                | String   | 0x87870bca...          | Contract address of the mainnet pool                        |
principal_reserve_decimals          | Int      | 18                     | Number of decimals used to store reserves data as integers  |
principal_reserve_name              | String   | Dai Stablecoin         | Name of the liquidation's debt reserve                      |
principalReserve.underlyingAsset    | String   | 0x6b175474e...         | Contract address of the debt underlying asset               |
user_id                             | String   | 0x23...                | Address of the user being liquidated                        |
import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
from subgrounds.subgrounds import Subgrounds
import utils
import asyncio

from string import Template

# using secrets file for general env vars https://docs.streamlit.io/streamlit-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management
NUMBER_OF_ACCOUNTS = st.secrets["NUMBER_OF_ACCOUNTS"]
AAVE_SUBGRAPH = st.secrets["AAVE_SUBGRAPH"]

sg = Subgrounds()
aaveV2_ext = sg.load_subgraph(AAVE_SUBGRAPH)

top_deposits = aaveV2_ext.Query.deposits(
    orderBy = "amountUSD",
    orderDirection = "desc",
    first=1000
)

large_deposits_df = sg.query_df([top_deposits.account.id])

accounts_to_consider_deposits = large_deposits_df['deposits_account_id'].unique()[:2*NUMBER_OF_ACCOUNTS]

# deposits
df_list = []
for account in accounts_to_consider_deposits:
    # couldn't figure out how to get these in a single query with subgrounds
    deposits_by_user = aaveV2_ext.Query.deposits(
        where=[aaveV2_ext.Query.account == account],
        orderBy = "amountUSD",
        orderDirection = "desc",
        first=100
    )

    deposits_df = sg.query_df([deposits_by_user.account.id, deposits_by_user.amountUSD, deposits_by_user.amount, deposits_by_user.asset.symbol, deposits_by_user.asset.id])
    withdraws_by_user = aaveV2_ext.Query.withdraws(
        where=[aaveV2_ext.Query.account == account],
        orderBy = "amountUSD",
        orderDirection = "desc",
        first=100
    )

    withdraws_df = sg.query_df([withdraws_by_user.account.id, withdraws_by_user.amountUSD, withdraws_by_user.amount, withdraws_by_user.asset.symbol, withdraws_by_user.asset.id])

    liquidations_by_user = aaveV2_ext.Query.liquidates(
        where=[aaveV2_ext.Query.liquidates.liquidatee == account],
        orderBy = "amountUSD",
        orderDirection = "desc",
        first=100
    )

    liquidations_df = sg.query_df([liquidations_by_user.liquidatee.id, liquidations_by_user.amountUSD, liquidations_by_user.amount, liquidations_by_user.asset.symbol, liquidations_by_user.asset.id])

    # sum deposits by token
    deposits_grouped = deposits_df.groupby(["deposits_account_id", "deposits_asset_id"], as_index=False)["deposits_amount"].sum()
    # sum withdraws by token
    if not withdraws_df.empty:
        withdraws_grouped = withdraws_df.groupby(["withdraws_account_id", "withdraws_asset_id"], as_index=False)["withdraws_amount"].sum()
    else:
        withdraws_grouped = pd.DataFrame(columns=["withdraws_account_id", "withdraws_asset_id", "withdraws_amount"])
    # sum liquidations by token
    if not liquidations_df.empty:
        liquidations_grouped = liquidations_df.groupby(["liquidations_account_id", "liquidations_asset_id"], as_index=False)["liquidations_amount"].sum()
    else:
        liquidations_grouped = pd.DataFrame(columns=["liquidations_account_id", "liquidations_asset_id", "liquidations_amount"])

    df = pd.merge(deposits_grouped, withdraws_grouped, how="left", left_on=["deposits_account_id", "deposits_asset_id"], right_on=["withdraws_account_id", "withdraws_asset_id"])
    df = pd.merge(df, liquidations_grouped, how="left", left_on=["deposits_account_id", "deposits_asset_id"], right_on=["liquidations_account_id", "liquidations_asset_id"])

    df_list.append(df)

all_users_deposits_df = pd.concat(df_list)

# ### Get Token Prices
token_list = all_users_deposits_df["deposits_asset_id"].unique()


token_prices_dict = asyncio.run(utils.get_token_prices(token_list, AAVE_SUBGRAPH))

asset_prices = pd.json_normalize(token_prices_dict)
asset_prices.rename(columns={"data.market.inputToken.id": "id", 
                            "data.market.inputToken.name": "name", 
                            "data.market.inputToken.symbol": "symbol", 
                            "data.market.inputToken.decimals": "decimals", 
                            "data.market.inputTokenPriceUSD": "price"}, inplace=True)

asset_prices["price"] = pd.to_numeric(asset_prices["price"], downcast="float")

# ### Get net deposits in native tokens

all_users_deposits_df["net_deposits"] = all_users_deposits_df["deposits_amount"].sub(all_users_deposits_df["withdraws_amount"], fill_value=0).sub(all_users_deposits_df["liquidations_amount"], fill_value=0)

# ### Merge net deposits with token data

net_deposits = pd.merge(all_users_deposits_df, asset_prices, left_on="deposits_asset_id", right_on="id")

net_deposits['net_deposits_usd'] = (net_deposits["net_deposits"] / (10 ** net_deposits["decimals"])) * net_deposits["price"]

net_deposits_by_user = net_deposits.groupby(["deposits_account_id"], as_index=False)["net_deposits_usd"].sum()

# TODO only count deposits where net deposit > 0
asset_count_by_user = net_deposits.groupby(["deposits_account_id"], as_index=False)["deposits_asset_id"].count()

net_deposits_asset_count_by_user = pd.merge(net_deposits_by_user, asset_count_by_user, on="deposits_account_id")

top_depositors = net_deposits_asset_count_by_user.sort_values(["net_deposits_usd"], ascending=False)[:NUMBER_OF_ACCOUNTS]


top_depositors["deposits_account_id"] = top_depositors["deposits_account_id"].apply(utils.convert_address_to_link, url_root="wallet_details?address=", target="_self")
top_depositors = top_depositors.to_html(escape=False)
st.set_page_config(layout="wide")
st.title("Avalanche AAVE V2 Whales")
st.subheader("Top Depositors")
st.write(top_depositors, unsafe_allow_html=True)

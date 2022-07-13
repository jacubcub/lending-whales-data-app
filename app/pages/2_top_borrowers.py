import streamlit as st
import pandas as pd
import requests
from subgrounds.subgrounds import Subgrounds
from string import Template
import utils
import asyncio

# using secrets file for general env vars https://docs.streamlit.io/streamlit-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management
NUMBER_OF_ACCOUNTS = st.secrets["NUMBER_OF_ACCOUNTS"]
AAVE_SUBGRAPH = st.secrets["AAVE_SUBGRAPH"]

sg = Subgrounds()
aaveV2_ext = sg.load_subgraph(AAVE_SUBGRAPH)

top_borrows = aaveV2_ext.Query.borrows(
    orderBy = "amountUSD",
    orderDirection = "desc",
    first=1000
)

large_borrows_df = sg.query_df([top_borrows.account.id])

accounts_to_consider_borrows = large_borrows_df["borrows_account_id"].unique()[:2*NUMBER_OF_ACCOUNTS]

# borrows
df_list = []
for account in accounts_to_consider_borrows:
    # couldn't figure out how to get these in a single query with subgrounds
    borrows_by_user = aaveV2_ext.Query.borrows(
        where=[aaveV2_ext.Query.account == account],
        orderBy = "amountUSD",
        orderDirection = "desc",
        first=100
    )

    borrows_df = sg.query_df([borrows_by_user.account.id, borrows_by_user.amountUSD, borrows_by_user.amount, borrows_by_user.asset.symbol, borrows_by_user.asset.id])

    repays_by_user = aaveV2_ext.Query.repays(
        where=[aaveV2_ext.Query.account == account],
        orderBy = "amountUSD",
        orderDirection = "desc",
        first=100
    )

    repays_df = sg.query_df([repays_by_user.account.id, repays_by_user.amountUSD, repays_by_user.amount, repays_by_user.asset.symbol, repays_by_user.asset.id])

    # sum borrows by token
    borrows_grouped = borrows_df.groupby(["borrows_account_id", "borrows_asset_id"], as_index=False)["borrows_amount"].sum()
    # sum repays by token
    if not repays_df.empty:
        repays_grouped = repays_df.groupby(["repays_account_id", "repays_asset_id"], as_index=False)["repays_amount"].sum()
    else:
        repays_grouped = pd.DataFrame(columns=["repays_account_id", "repays_asset_id", "repays_amount"])

    df = pd.merge(borrows_grouped, repays_grouped, how="left", left_on=["borrows_account_id", "borrows_asset_id"], right_on=["repays_account_id", "repays_asset_id"])
    df_list.append(df)

all_users_borrows_df = pd.concat(df_list)

# Get Token Prices
token_list = all_users_borrows_df["borrows_asset_id"].unique()


token_prices_dict = asyncio.run(utils.get_token_prices(token_list, AAVE_SUBGRAPH))

asset_prices = pd.json_normalize(token_prices_dict)
asset_prices.rename(columns={"data.market.inputToken.id": "id", 
                            "data.market.inputToken.name": "name", 
                            "data.market.inputToken.symbol": "symbol", 
                            "data.market.inputToken.decimals": "decimals", 
                            "data.market.inputTokenPriceUSD": "price"}, inplace=True)

asset_prices["price"] = pd.to_numeric(asset_prices["price"], downcast="float")

# ### Get net borrows in native tokens

all_users_borrows_df["net_borrows"] = all_users_borrows_df["borrows_amount"].sub(all_users_borrows_df["repays_amount"], fill_value=0)

# ### Merge net borrows with token data

net_borrows = pd.merge(all_users_borrows_df, asset_prices, left_on="borrows_asset_id", right_on="id")

net_borrows['net_borrows_usd'] = (net_borrows["net_borrows"] / (10 ** net_borrows["decimals"])) * net_borrows["price"]

net_borrows_by_user = net_borrows.groupby(["borrows_account_id"], as_index=False)["net_borrows_usd"].sum()

# TODO only count borrows where net deposit > 0
asset_count_by_user = net_borrows.groupby(["borrows_account_id"], as_index=False)["borrows_asset_id"].count()

net_borrows_asset_count_by_user = pd.merge(net_borrows_by_user, asset_count_by_user, on="borrows_account_id")

top_borrowers = net_borrows_asset_count_by_user.sort_values(["net_borrows_usd"], ascending=False)[:NUMBER_OF_ACCOUNTS]



top_borrowers["borrows_account_id"]= top_borrowers["borrows_account_id"].apply(utils.convert_address_to_link, url_root="wallet_details?address=", target="_self")
top_borrowers = top_borrowers.to_html(escape=False)

st.set_page_config(layout="wide")
st.title("Avalanche AAVE V2 Whales")
st.subheader("Top Borrowers")
st.write(top_borrowers, unsafe_allow_html=True)

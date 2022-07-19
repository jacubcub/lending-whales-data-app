from pyrsistent import get_in
import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
from subgrounds.subgrounds import Subgrounds
import utils
import asyncio

from string import Template

st.set_page_config(page_title="Whale Watcher", page_icon="🐋", layout="wide")
st.title("🐋 Whale Watcher")
st.text("AAVE V2 on Avalanche")

# using secrets file for general env vars https://docs.streamlit.io/streamlit-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management
url = st.secrets["AAVE_SUBGRAPH"]

sg = Subgrounds()
lending = sg.load_subgraph(url)

@st.experimental_memo
def get_initial_data():
    return utils.get_all_open_positions(url)

open_positions_df = get_initial_data()

whale_type_select = st.selectbox("Show Top 100", ('Depositors', 'Borrowers'))

if whale_type_select == "Depositors":
    position_side = "LENDER"
    position_side_column_label = "CURRENT DEPOSITS"
    number_assets_column_label = "NO. OF UNIQUE ASSETS DEPOSITED"
else:
    position_side = "BORROWER"
    position_side_column_label = "CURRENT BORROWS"
    number_assets_column_label = "NO. OF UNIQUE ASSETS BORROWED"

sided_df = open_positions_df[(open_positions_df["side"] == position_side)]

agg_df = sided_df.groupby("account_id", as_index=False).agg(
    usd_value=('balance_usd', 'sum'),
    asset_count=('market.inputToken.symbol', 'count')
    ).sort_values(
        ["usd_value"], 
        ascending=False
    )[:100].reset_index(drop=True)


agg_df["usd_value"] = agg_df["usd_value"].apply(lambda x: "${:,.0f}".format(x))
agg_df.rename(columns={"account_id": "ADDRESS", "usd_value": position_side_column_label, "asset_count": number_assets_column_label}, inplace=True)

selection = utils.aggrid_interactive_table(agg_df)

if st.button("Clear Cached Data"):
    get_initial_data.clear()
    get_initial_data()

if selection:
    try:
        selected_address = selection["selected_rows"][0]["ADDRESS"]
        # DEPOSITS
        st.write("Deposits -", selected_address)
        address_deposit_positions = open_positions_df[(open_positions_df["side"] == "LENDER") & (open_positions_df["account_id"] == selected_address)]
        display_user_deposits_df = address_deposit_positions[["market.inputToken.symbol", "balance_adj", "market.inputTokenPriceUSD", "balance_usd"]].copy()
        display_user_deposits_df["balance_usd"] = display_user_deposits_df["balance_usd"].apply(lambda x: "${:,.0f}".format(x))
        display_user_deposits_df["balance_adj"] = display_user_deposits_df["balance_adj"].apply(lambda x: "{:,.2f}".format(x))
        display_user_deposits_df["market.inputTokenPriceUSD"] = display_user_deposits_df["market.inputTokenPriceUSD"].apply(lambda x: "${:,.2f}".format(x))
        display_user_deposits_df.rename(columns={
            "market.inputToken.symbol": "ASSET", "balance_adj": "DEPOSIT AMOUNT", "market.inputTokenPriceUSD": "CURRENT PRICE",
            "balance_usd": "TOTAL DEPOSIT VALUE"}, inplace=True)
        st.write(display_user_deposits_df)
        #BORROWS
        st.write("Borrows -", selected_address)
        address_borrow_positions = open_positions_df[(open_positions_df["side"] == "BORROWER") & (open_positions_df["account_id"] == selected_address)]
        display_user_borrows_df = address_borrow_positions[["market.inputToken.symbol", "market.inputTokenPriceUSD", "balance_adj", "balance_usd"]].copy()
        display_user_borrows_df["balance_usd"] = display_user_borrows_df["balance_usd"].apply(lambda x: "${:,.0f}".format(x))
        display_user_borrows_df["balance_adj"] = display_user_borrows_df["balance_adj"].apply(lambda x: "{:,.2f}".format(x))
        display_user_borrows_df["market.inputTokenPriceUSD"] = display_user_borrows_df["market.inputTokenPriceUSD"].apply(lambda x: "${:,.2f}".format(x))
        display_user_borrows_df.rename(columns={
            "market.inputToken.symbol": "ASSET", "balance_adj": "BORROWED AMOUNT", "market.inputTokenPriceUSD": "CURRENT PRICE",
            "balance_usd": "TOTAL BORROWED VALUE"}, inplace=True)
        st.write(display_user_borrows_df)
    except IndexError:
        st.write("Select a row in the table to view detailed lending data for that address.")

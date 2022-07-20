from pyrsistent import get_in
import streamlit as st
import pandas as pd
import requests
import plotly.graph_objects as go
from subgrounds.subgrounds import Subgrounds
import utils
from millify import millify

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
    block = utils.get_lastest_synced_block_number(url)
    return utils.get_all_open_positions(url, block)

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
        address_deposit_positions = open_positions_df[(open_positions_df["side"] == "LENDER") & (open_positions_df["account_id"] == selected_address)].copy()
        current_deposited_metric = address_deposit_positions["balance_usd"].sum()
        address_deposit_positions["percent_of_total_deposits"] = address_deposit_positions["balance_usd"] / address_deposit_positions["totalDepositBalanceUSD"]
        display_user_deposits_df = address_deposit_positions[["market.inputToken.symbol", "balance_adj", "market.inputTokenPriceUSD", "balance_usd", "percent_of_total_deposits", "lender_variable_rate"]].copy()
        display_user_deposits_df["balance_usd"] = display_user_deposits_df["balance_usd"].apply(lambda x: "${:,.0f}".format(x))
        display_user_deposits_df["balance_adj"] = display_user_deposits_df["balance_adj"].apply(lambda x: "{:,.2f}".format(x))
        display_user_deposits_df["market.inputTokenPriceUSD"] = display_user_deposits_df["market.inputTokenPriceUSD"].apply(lambda x: "${:,.2f}".format(x))
        display_user_deposits_df["percent_of_total_deposits"] = display_user_deposits_df["percent_of_total_deposits"].apply(lambda x: "{:,.2f}%".format(x))
        display_user_deposits_df["lender_variable_rate"] = display_user_deposits_df["lender_variable_rate"].apply(lambda x: "{:,.2f}%".format(x))
        display_user_deposits_df.rename(columns={
            "market.inputToken.symbol": "ASSET", "balance_adj": "DEPOSIT AMOUNT", "market.inputTokenPriceUSD": "CURRENT PRICE",
            "balance_usd": "TOTAL DEPOSIT VALUE", "percent_of_total_deposits": "% OF TOTAL DEPOSITS", "lender_variable_rate": "APY"}, inplace=True)
        #BORROWS
        address_borrow_positions = open_positions_df[(open_positions_df["side"] == "BORROWER") & (open_positions_df["account_id"] == selected_address)].copy()
        current_borrowed_metric = address_borrow_positions["balance_usd"].sum()
        address_borrow_positions["percent_of_total_borrows"] = address_borrow_positions["balance_usd"] / address_borrow_positions["totalBorrowBalanceUSD"]
        display_user_borrows_df = address_borrow_positions[["market.inputToken.symbol", "market.inputTokenPriceUSD", "balance_adj", "balance_usd", "percent_of_total_borrows", "borrower_variable_rate", "borrower_stable_rate"]].copy()
        display_user_borrows_df["balance_usd"] = display_user_borrows_df["balance_usd"].apply(lambda x: "${:,.0f}".format(x))
        display_user_borrows_df["balance_adj"] = display_user_borrows_df["balance_adj"].apply(lambda x: "{:,.2f}".format(x))
        display_user_borrows_df["market.inputTokenPriceUSD"] = display_user_borrows_df["market.inputTokenPriceUSD"].apply(lambda x: "${:,.2f}".format(x))
        display_user_borrows_df["percent_of_total_borrows"] = display_user_borrows_df["percent_of_total_borrows"].apply(lambda x: "{:,.2f}%".format(x))
        display_user_borrows_df["borrower_variable_rate"] = display_user_borrows_df["borrower_variable_rate"].apply(lambda x: "{:,.2f}%".format(x))
        display_user_borrows_df["borrower_stable_rate"] = display_user_borrows_df["borrower_stable_rate"].apply(lambda x: "{:,.2f}%".format(x))
        display_user_borrows_df.rename(columns={
            "market.inputToken.symbol": "ASSET", "balance_adj": "BORROWED AMOUNT", "market.inputTokenPriceUSD": "CURRENT PRICE",
            "balance_usd": "TOTAL BORROWED VALUE", "percent_of_total_borrows": "% OF TOTAL BORROWS", "borrower_variable_rate": "APY VARIABLE", "borrower_stable_rate": "APY STABLE"}, inplace=True)
        st.write('<hr/>', unsafe_allow_html=True)
        st.subheader(selected_address)
        col1, col2, col3 = st.columns(3)
        col1.metric("Current Deposits", "$" + millify(current_deposited_metric, precision=2))
        col2.metric("Current Borrowed", "$" + millify(current_borrowed_metric, precision=2))
        # col3.metric("Humidity", "86%", "4%")
        st.write("Deposits")        
        st.write(display_user_deposits_df)
        st.write("Borrows")
        st.write(display_user_borrows_df)
    except IndexError:
        st.write("Select a row in the table to view detailed lending data for that address.")

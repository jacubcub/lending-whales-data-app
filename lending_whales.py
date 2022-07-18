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

depositor_df = open_positions_df[(open_positions_df["side"] == position_side)]

agg_df = depositor_df.groupby("account_id", as_index=False).agg(
    usd_value=('balance_usd', 'sum'),
    asset_count=('market.inputToken.symbol', 'count')
    ).sort_values(
        ["usd_value"], 
        ascending=False
    )[:100].reset_index(drop=True)


agg_df["usd_value"] = agg_df["usd_value"].apply(lambda x: "${:,.2f}".format(x))
agg_df.rename(columns={"account_id": "ADDRESS", "usd_value": position_side_column_label, "asset_count": number_assets_column_label}, inplace=True)

st.write(agg_df)

if st.button("Clear Cached Data"):
    get_initial_data.clear()
    get_initial_data()
    
import pandas as pd
import requests
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode

def convert_address_to_link(address: str, url_root: str, target: str = "_blank") -> str:
    """Creates an HTML link by appending address to url

    Args:
        address (str): Text that will become the clickable link
        url_root (str): URL root that address will be appended to
        target (str): Target of HTML <a> tag
            (default is "_blank")

    Returns:
        str: HTML anchor element link
    """
    return f'<a target="{target}" href="{url_root}{address}">{address}</a>'

def get_lastest_synced_block_number(subgraph_url: str) -> int:
    latest_block_query = """
        query {
            _meta {
                block {
                number
                }
            }
        }
    """
    payload = {
        "query": latest_block_query
    }
    resp = requests.post(subgraph_url, json=payload)
    data = resp.json()
    return data["data"]["_meta"]["block"]["number"]

def _query_position_market_data(subgraph_url: str, block_num: int):
    last_id = "0x0000000000000000000000000000000000000000"
    data_list = []
    first = 500

    while True:
        all_positions_query = """
            query($first: Int, $last_id: String, $block_num: Int){
                accounts(first: $first, where: {openPositionCount_gt: 0, id_gt: $last_id}, orderBy: id, block: {number: $block_num}) {
                    account_id: id
                    positions(where: {hashClosed: null}) {
                        balance
                        side
                        market {
                            market_id: id
                            inputTokenPriceUSD
                            inputToken {
                                symbol
                                decimals
                            }
                            rates {
                                rate
                                rate_side: side
                                rate_type: type
                            }
                            dailySnapshots(first: 1, orderBy: timestamp, orderDirection: desc) {
                                totalBorrowBalanceUSD
                                totalDepositBalanceUSD
                            }
                        }
                    }
                }
            }
            """

        payload = {
            "query": all_positions_query, 
            "variables": {
                "first": first,
                "last_id": last_id,
                "block_num": block_num
            }
        }
        resp = requests.post(subgraph_url, json=payload)
        data = resp.json()
        data_list.extend(data["data"]["accounts"])

        if (len(data["data"]["accounts"]) != first):
            return data_list

        # get into df to get max account_id
        tmp_df = pd.json_normalize(data["data"]["accounts"], ["positions"], ["account_id"])
        last_id = tmp_df["account_id"].max()
        print("Progress: ", "{:.1%}".format(int(last_id[:5], 16) / 0xfff), end="\r", flush=True)
        

def get_all_open_positions(subgraph_url: str, block_num: int) -> pd.DataFrame:
    """Gets all open positions from extended lending subgraph

    Args:
        subgraph_url (str): URL of extended lending subgraph
        block_num (int): block height to query

    Returns:
        pd.DataFrame: Pandas DataFrame of positions with columns
            ['account_id', 'side', 'market.inputTokenPriceUSD', 'market.inputToken.symbol', 
            'market.market_id', 'balance_adj', 'balance_usd', 'totalBorrowBalanceUSD', 'totalDepositBalanceUSD',
            'borrower_stable_rate', 'borrower_variable_rate', 'lender_variable_rate']
    """

    data_list = _query_position_market_data(subgraph_url=subgraph_url, block_num=block_num)
    
    positions_df = pd.json_normalize(data_list, ["positions"], ["account_id"])
    positions_df["balance"] = positions_df["balance"].apply(int) # numbers too large for pd.to_numeric()
    positions_df["market.inputTokenPriceUSD"] = pd.to_numeric(positions_df["market.inputTokenPriceUSD"])
    positions_df["balance_adj"] = positions_df["balance"] / (10 ** positions_df["market.inputToken.decimals"])
    positions_df["balance_usd"] = positions_df["balance_adj"] * positions_df["market.inputTokenPriceUSD"]
    # don't need these anymore and balance will just cause issues due to large numbers
    positions_df.drop(columns=["balance", "market.inputToken.decimals", "market.rates", "market.dailySnapshots"], inplace=True)

    snapshots_df = pd.json_normalize(data_list, record_path=["positions", "market", "dailySnapshots"], meta=[["positions", "market", "market_id"]])
    snapshots_df = snapshots_df.drop_duplicates()

    rates_df = pd.json_normalize(data["data"]["accounts"], record_path=["positions", "market", "rates"], meta=[["positions", "market", "market_id"]])
    rates_df = rates_df.drop_duplicates()
    rates_pivot = rates_df.pivot(index='positions.market.market_id', columns=['rate_side', 'rate_type'], values='rate')
    rates_data = {
        'positions.market.market_id': rates_pivot.index, 
        'borrower_stable_rate': rates_pivot["BORROWER"].reset_index()["STABLE"], 
        'borrower_variable_rate': rates_pivot["BORROWER"].reset_index()["VARIABLE"], 
        'lender_variable_rate': rates_pivot["LENDER"].reset_index()["VARIABLE"]}
    rates_flat_df = pd.DataFrame(rates_data)
    positions_markets_rates_df = positions_df.merge(snapshots_df, left_on="market.market_id", right_on="positions.market.market_id").merge(rates_flat_df, left_on="market.market_id", right_on="positions.market.market_id")
    results_df = positions_markets_rates_df[[
        'account_id', 'side', 'market.inputTokenPriceUSD', 'market.inputToken.symbol', 
        'market.market_id', 'balance_adj', 'balance_usd', 'totalBorrowBalanceUSD', 'totalDepositBalanceUSD',
        'borrower_stable_rate', 'borrower_variable_rate', 'lender_variable_rate']]
    results_df[["borrower_stable_rate", "borrower_variable_rate", "lender_variable_rate", "totalBorrowBalanceUSD", "totalDepositBalanceUSD"]] = results_df[["borrower_stable_rate", "borrower_variable_rate", "lender_variable_rate", "totalBorrowBalanceUSD", "totalDepositBalanceUSD"]].copy().apply(pd.to_numeric)
    return results_df


def aggrid_interactive_table(df: pd.DataFrame):
    """Creates an st-aggrid interactive table based on a dataframe.

    Args:
        df (pd.DataFrame]): Source dataframe

    Returns:
        dict: The selected row
    """
    options = GridOptionsBuilder.from_dataframe(
        df, enableRowGroup=True, enableValue=True, enablePivot=True
    )

    options.configure_selection("single")
    selection = AgGrid(
        df,
        enable_enterprise_modules=True,
        gridOptions=options.build(),
        theme="dark",
        update_mode=GridUpdateMode.MODEL_CHANGED,
        allow_unsafe_jscode=True,
    )

    return selection

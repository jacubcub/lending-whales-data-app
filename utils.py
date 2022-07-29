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


def _get_daily_snapshot_blocks(subgraph_url: str, days_back: int) -> pd.DataFrame:
    """Gets block numbers corresponding to each daily snapshot
    
    Args:
        subgraph_url (str): URL of extended lending subgraph
        days_back (int): Number of days back to query (i.e. 30 will return block numbers of previous 30 days)

    Returns:
        pd.DataFrame: Pandas DataFrame of block numbers, timestamps, and dates
    """

    query = """
        query($days_back: Int){
            financialsDailySnapshots(first: $days_back, orderBy: timestamp, orderDirection: desc) {
                blockNumber
                timestamp
            }
        }
    """

    payload = {
        "query": query, 
        "variables": {
            "days_back": days_back
        }
    }

    resp = requests.post(subgraph_url, json=payload)
    data = resp.json()
    df = pd.json_normalize(data['data']['financialsDailySnapshots'])
    df['blockNumber'] = pd.to_numeric(df['blockNumber'])
    df['date'] = pd.to_datetime(df['timestamp'], unit='s')
    return df


def get_account_daily_positions(subgraph_url: str, account_id: str, days_back: int) -> pd.DataFrame:
    """Gets daily historical open positions for a given account going back the specified number of days"""

    snapshot_blocks_df = _get_daily_snapshot_blocks(subgraph_url, days_back)
    data_list = []

    for index, block in enumerate(snapshot_blocks_df["blockNumber"], start=1):
        query = """
            query($account_id: String, $block_num: Int){
                accounts(
                    where: {openPositionCount_gt: 0, id: $account_id}
                    block: {number: $block_num}
                ) {
                    account_id: id
                    positions(where: {hashClosed: null}) {
                        balance
                        side
                        market {
                            market_id: id
                            inputTokenPriceUSD
                            inputToken {
                                decimals
                                symbol
                            }
                            # rates {
                            #     rate
                            #     rate_side: side
                            #     rate_type: type
                            # }
                            # dailySnapshots(first: 1, orderBy: timestamp, orderDirection: desc) {
                            #     totalBorrowBalanceUSD
                            #     totalDepositBalanceUSD
                            # }
                        }
                    }
                }
            }
        """

        payload = {
            "query": query,
            "variables": {
                "account_id": account_id,
                "block_num": block
            }
        }

        resp = requests.post(subgraph_url, json=payload)
        print("Progress: Day", index, "of",  days_back, end="\r", flush=True)
        data = resp.json()
        data["data"]["accounts"][0]["block_number"] = block
        data_list.extend(data["data"]["accounts"])
        
    positions_df = pd.json_normalize(data_list, ["positions"], ["account_id", "block_number"])
    positions_df["balance"] = positions_df["balance"].apply(int) # numbers too large for pd.to_numeric()
    positions_df["market.inputTokenPriceUSD"] = pd.to_numeric(positions_df["market.inputTokenPriceUSD"])
    positions_df["balance_adj"] = positions_df["balance"] / (10 ** positions_df["market.inputToken.decimals"])
    positions_df["balance_usd"] = positions_df["balance_adj"] * positions_df["market.inputTokenPriceUSD"]
    # don't need these anymore and balance will just cause issues due to large numbers
    positions_df.drop(columns=["balance", "market.inputToken.decimals"], inplace=True)

    lender_amounts_by_block = positions_df[(positions_df["side"] == "LENDER")].groupby(["block_number"], as_index=False)["balance_usd"].sum()
    lender_amounts_by_block.rename(columns={"block_number": "blockNumber", "balance_usd": "deposits_usd"}, inplace=True)
    if 'deposits_usd' not in lender_amounts_by_block.columns:
        lender_amounts_by_block['deposits_usd'] = None
    borrower_amounts_by_block = positions_df[(positions_df["side"] == "BORROWER")].groupby(["block_number"], as_index=False)["balance_usd"].sum()
    borrower_amounts_by_block.rename(columns={"block_number": "blockNumber", "balance_usd": "borrows_usd"}, inplace=True)
    if 'borrows_usd' not in borrower_amounts_by_block.columns:
        borrower_amounts_by_block['borrows_usd'] = None

    ts_account_positions_df = snapshot_blocks_df.merge(borrower_amounts_by_block, how="left", on="blockNumber")
    ts_account_positions_df = ts_account_positions_df.merge(lender_amounts_by_block, how="left", on="blockNumber")
    ts_account_positions_df.fillna(0, inplace=True)
    return ts_account_positions_df


def _query_position_market_data(subgraph_url: str, block_num: int):
    last_id = "0x0000000000000000000000000000000000000000"
    data_list = []
    first = 500

    while True:
        # add parameter to filter by account
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

    rates_df = pd.json_normalize(data_list, record_path=["positions", "market", "rates"], meta=[["positions", "market", "market_id"]])
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


def get_account_events(subgraph_url: str, account_id: str) -> pd.DataFrame:

    query = """
    query($account_id: String){
    accounts(where: {id: $account_id}) {
        account_id: id
        borrows {
        amount
        amountUSD
        asset {
            symbol
            decimals
        }
        timestamp
        }
        deposits {
        amount
        amountUSD
        asset {
            symbol
            decimals
        }
        timestamp
        }
        liquidates {
        amount
        amountUSD
        asset {
            symbol
            decimals
        }
        timestamp
        }
        liquidations {
        amount
        amountUSD
        asset {
            decimals
            symbol
        }
        timestamp
        }
        repays {
        amount
        amountUSD
        asset {
            decimals
            symbol
        }
        timestamp
        }
        withdraws {
        amount
        amountUSD
        asset {
            decimals
            symbol
        }
        timestamp
        }
    }
    }
    """

    payload = {
            "query": query,
            "variables": {
                "account_id": account_id
            }
        }

    resp = requests.post(subgraph_url, json=payload)
    # print("Progress: Day", index, "of",  days_back, end="\r", flush=True)
    data = resp.json()

    deposits_df = pd.json_normalize(data["data"]["accounts"][0], ["deposits"])
    deposits_df["event"] = "deposit"
    borrows_df = pd.json_normalize(data["data"]["accounts"][0], ["borrows"])
    borrows_df["event"] = "borrow"
    withdraws_df = pd.json_normalize(data["data"]["accounts"][0], ["withdraws"])
    withdraws_df["event"] = "withdraw"
    liquidates_df = pd.json_normalize(data["data"]["accounts"][0], ["liquidates"])
    liquidates_df["event"] = "liquidate"
    liquidations_df = pd.json_normalize(data["data"]["accounts"][0], ["liquidations"])
    liquidations_df["event"] = "liquidation"
    repays_df = pd.json_normalize(data["data"]["accounts"][0], ["repays"])
    repays_df["event"] = "repay"

    events_df = pd.concat([deposits_df, borrows_df, withdraws_df, liquidates_df, liquidations_df, repays_df], ignore_index=True).sort_values("timestamp", ascending=False)
    events_df['date'] = pd.to_datetime(events_df['timestamp'], unit='s')
    events_df = events_df.set_index("date")

    events_df["amountUSD"] = pd.to_numeric(events_df["amountUSD"])
    events_df["amount"] = events_df["amount"].apply(int) # numbers too large for pd.to_numeric()
    events_df["amount_adj"] = events_df["amount"] / (10 ** events_df["asset.decimals"])
    # don't need these anymore and balance will just cause issues due to large numbers
    events_df.drop(columns=["timestamp", "asset.decimals", "amount"], inplace=True)
    events_df = events_df[['event', 'asset.symbol', 'amount_adj', 'amountUSD']]

    return events_df


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

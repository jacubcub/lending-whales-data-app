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


def get_all_open_positions(subgraph_url: str) -> pd.DataFrame:
    """Gets all open positions from extended lending subgraph

    Args:
        subgraph_url (str): URL of extended lending subgraph

    Returns:
        pd.DataFrame: Pandas DataFrame of positions with columns
            ['side', 'market.inputTokenPriceUSD',
            'market.inputToken.symbol', 'account_id', 'balance_adj', 'balance_usd]
    """
    last_id = "0x0000000000000000000000000000000000000000"
    data_list = []
    first = 500

    while True:
        all_positions_query = """
            query($first: Int, $last_id: String){
                accounts(first: $first, where: {openPositionCount_gt: 0, id_gt: $last_id}, orderBy: id) {
                    account_id: id
                    positions(where: {hashClosed: null}) {
                        balance
                        side
                        market {
                            inputTokenPriceUSD
                            inputToken {
                                symbol
                                decimals
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
                "last_id": last_id
            }
        }
        resp = requests.post(subgraph_url, json=payload)
        data = resp.json()
        data_list.extend(data["data"]["accounts"])

        if (len(data["data"]["accounts"]) != first):
            break

        # get into df to get max account_id
        tmp_df = pd.json_normalize(data["data"]["accounts"], "positions", ["account_id"])
        last_id = tmp_df["account_id"].max()
        print("Progress: ", "{:.1%}".format(int(last_id[:5], 16) / 0xfff), end="\r", flush=True)
    
    positions_df = pd.json_normalize(data_list, "positions", ["account_id"])
    positions_df["balance"] = positions_df["balance"].apply(int) # numbers too large for pd.to_numeric()
    positions_df["market.inputTokenPriceUSD"] = pd.to_numeric(positions_df["market.inputTokenPriceUSD"])
    positions_df["balance_adj"] = positions_df["balance"] / (10 ** positions_df["market.inputToken.decimals"])
    positions_df["balance_usd"] = positions_df["balance_adj"] * positions_df["market.inputTokenPriceUSD"]
    # don't need these anymore and balance will just cause issues due to large numbers
    positions_df.drop(columns=["balance", "market.inputToken.decimals"], inplace=True)
    return positions_df


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

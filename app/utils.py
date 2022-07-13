import pandas as pd
import asyncio
import aiohttp
from string import Template

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


async def get_token_price(session: aiohttp.ClientSession, token_id: str, url: str) -> dict:
    """get token price"""
    str = Template("""
    {
    market(id: "$token_id") {
        inputToken {
            id
            name
            symbol
            decimals
        }
        inputTokenPriceUSD
        }
    }
    """).substitute(token_id=token_id)
    payload = {
    'query': 
    str        
    }
    resp = await session.request('POST', url=url, json=payload)
    data = await resp.json()
    return data


async def get_token_prices(token_ids: list, url: str) -> list:
    async with aiohttp.ClientSession() as session:
        tasks = []
        for token_id in token_ids:
            tasks.append(get_token_price(session=session, token_id=token_id, url=url))
        token_prices = await asyncio.gather(*tasks, return_exceptions=True)
        return token_prices

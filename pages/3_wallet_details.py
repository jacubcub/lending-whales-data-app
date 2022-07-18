import streamlit as st
import pandas as pd
import requests

from string import Template

AAVE_SUBGRAPH = st.secrets["AAVE_SUBGRAPH"]

params = st.experimental_get_query_params()
try:
    address = params["address"][0]
except KeyError:
    st.error("Must include address in url")
    address = "0x0000000000000000000000000000000000000000"
st.write('address is: ' + address)

query = Template("""
query markets {
  accounts(where: {id: "$address"}) {
    deposits {
      amount
      asset {
        id
        symbol
      }
    }
    liquidations {
      amount
      asset {
        id
        symbol
      }
    }
    repays {
      amount
      asset {
        id
        symbol
      }
    }
    borrows {
      amount
      asset {
        id
        symbol
      }
    }
    withdraws {
      amount
      asset {
        id
        symbol
      }
    }
  }
}
""").substitute(address=address)

payload = {"query": query}

resp = requests.post(AAVE_SUBGRAPH, json=payload).json()

st.write(resp)
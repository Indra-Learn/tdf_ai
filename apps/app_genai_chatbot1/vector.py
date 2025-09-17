from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir)))
from utils.stock_markets.nse_apis import NSE_APIS

nse_api = NSE_APIS()
# daily_market_updates = nse_api.get_daily_allIndices_data()
large_deals = nse_api.get_large_deal_data()
# fii_dii_trades = nse_api.get_daily_fii_dii_data()

# daily_market_updates_df = pd.DataFrame(daily_market_updates)
large_deals_df = pd.DataFrame(large_deals)
# fii_dii_trades_df = pd.DataFrame(fii_dii_trades)

embeddings = OllamaEmbeddings(model="mxbai-embed-large")

db_location = "./local_db/chrome_langchain_db"
add_documents = not os.path.exists(db_location)
if add_documents:
    documents = []
    ids = []

    for i, row in large_deals_df.iterrows():
        document = Document(
            page_content = f'''
                            Totay {str(row["symbol"])}-{str(row["name"])} company has been traded in large deal in india stock market, where number of buy trades are {str(row["buy_trades"])} 
                            with total volumn of {str(row["buy_qty_sum"])} and minimum of weighted average traded price is {str(row["buy_watp_min"])} and maximum of weighted average traded price is {str(row["buy_watp_max"])} 
                            and number of sell trades are {str(row["sell_trades"])} with total volumn of {str(row["sell_qty_sum"])} and minimum of weighted average traded price is {str(row["sell_watp_min"])} and maximum of weighted average traded price is {str(row["sell_watp_max"])}
                            ''',
            metadata = {"Company Symbol": row["symbol"], "Total Buy Volumn": row["buy_qty_sum"], "Total Sell Volumn": row["sell_qty_sum"], "Date": "15th Septemenber, 2025"},
            id=str(i)
        )
        ids.append(str(i))
        documents.append(document)

vector_store = Chroma(
    collection_name="large_deal_data",
    persist_directory=db_location,
    embedding_function=embeddings
)

if add_documents:
    vector_store.add_documents(documents=documents, ids=ids)

retriever = vector_store.as_retriever(
    search_kwargs={"k": 1}
)
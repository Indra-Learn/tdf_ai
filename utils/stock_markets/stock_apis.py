"""
https://github.com/zerodha/pykiteconnect/blob/master/examples/flask_app.py
https://kite.trade/docs/pykiteconnect/v4/
"""
import os, sys
import logging
from kiteconnect import KiteConnect
import requests
import pandas as pd
from flask import session

logging.basicConfig(level=logging.DEBUG)

parentdir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir, os.path.pardir))
sys.path.append(parentdir)

from utils.tdf_admin import get_kite_secret
from config import dev_config

HOST = dev_config["host"] 
PORT = dev_config["port"]

kite_secret = get_kite_secret()
kite_api_key = kite_secret["api_key"]

kite_login_url = "https://kite.zerodha.com/connect/login?api_key={api_key}".format(api_key=kite_api_key)
kite_redirect_url = "http://{host}:{port}/kitelogin".format(host=HOST, port=PORT)
kite_console_url = "https://developers.kite.trade/apps/{api_key}".format(api_key=kite_api_key)

def get_kite_client():
    kite = KiteConnect(api_key=kite_api_key)
    if "kite_access_token" in session:
        kite.set_access_token(session["kite_access_token"])
    return kite
    
def _get_data_from_kite_api(api_endpoint_url):
    data = {}
    data["data"] = ""
    if "kite_access_token" in session:
        kite_access_token = session["kite_access_token"]
        kite_root_url = "https://api.kite.trade"
        kite_headers = {
            "X-Kite-Version": "3",
            "Authorization": f"token {kite_api_key}:{kite_access_token}"
        }
        full_kite_url = kite_root_url + "/" + api_endpoint_url
        response = requests.get(url=full_kite_url, headers=kite_headers)
        logging.info(f"Calling Kite API: {full_kite_url}..")
        response.raise_for_status()
        data = response.json()
    return data["data"]


def get_mf_instrument():
    kite = KiteConnect(api_key=kite_api_key)
    # mf_instruments_df = pd.DataFrame(_get_data_from_kite_api("mf/instruments"))
    mf_instruments_df = pd.DataFrame(kite.mf_instruments())
    mf_instruments_df = (mf_instruments_df.loc[((mf_instruments_df["purchase_allowed"] == True) 
                                               & (mf_instruments_df["dividend_type"] == "growth")
                                               & (mf_instruments_df["plan"] == "direct"))
                                              , ["tradingsymbol", "amc", "name", "dividend_type", "scheme_type", "plan", "last_price", "last_price_date"]])
    return mf_instruments_df





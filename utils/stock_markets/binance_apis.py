import os
import sys
import requests
import logging
from dotenv import load_dotenv

# parentdir = os.path.abspath(os.path.join(os.path.dirname(__file__)), os.path.pardir)
# sys.path.append(parentdir)

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

x_rapid_api_key=os.getenv("X-RAPIDAPI-KEY")
x_rapidapi_host=os.getenv("X-RAPIDAPI-HOST")

class Binance_API:
    headers = {
        "X-RapidAPI-Key": x_rapid_api_key,
        "X-RapidAPI-Host": x_rapidapi_host
    }
    def __init__(self):
        pass
    def get_data(self, url):
        print(f"calling {url} ..")
        output = dict()
        try:
            response = requests.request("GET", url, headers=Binance_API.headers)
        except Exception as e:
            print(f"error from Binance_API.get_data(): {e}")
        else:
            if response.status_code == 200:
                output = response.json()
        return output
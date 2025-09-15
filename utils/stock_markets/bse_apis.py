"""
https://github.com/BennyThadikaran/BseIndiaApi/blob/main/src/bse/BSE.py
"""

import requests

class BSE_APIS(object):
    base_url = "https://www.bseindia.com/"
    api_url = "https://api.bseindia.com/BseIndiaAPI/api"
    
    def __init__(self):
        self.base_bse_url = "https://api.bseindia.com/"

    def _get_data(self, api_url):
        full_url = self.base_bse_url + api_url
        response = requests.get(full_url)
        print(response)
        return full_url

    def get_daily_sensex_data(self):
        data = self._get_data("BseIndiaAPI/api/NewFundsMobilizedFourFY/w")
        return data
    

if __name__ == "__main__":
    bse_api = BSE_APIS()
    data = bse_api.get_daily_sensex_data()
    print(f"{data}")
import requests
import pandas as pd

class NSE_APIS:
    base_nse_url = "https://www.nseindia.com/"
    nse_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    def __init__(self):
        self.nse_session = requests.Session()
        self.nse_session.headers.update(self.nse_headers)
        self.nse_session.get(self.base_nse_url, headers=self.nse_headers,  timeout=10)
        self.nse_session.get(self.base_nse_url+"/option-chain", headers=self.nse_headers,  timeout=10)

    def _get_data(self, api_url):
        full_nse_api_url = self.base_nse_url + api_url
        print(f"calling {full_nse_api_url} ..")
        output = dict()
        try:
            response = self.nse_session.get(full_nse_api_url)
            response.raise_for_status()
        except Exception as e:
            print(f"error from NSE_API.get_data(): {e}")
        else:
            if response.status_code == 200:
                output = response.json()
        return output
    
    def get_large_deal_data(self):
        data = self._get_data("api/snapshot-capital-market-largedeal")
        df = pd.DataFrame(data.get("BULK_DEALS_DATA"))
        df[["qty"]] = df[["qty"]].astype(int)
        df[["watp"]] = df[["watp"]].astype(float)
        grouped_df = df.groupby(["symbol", "name", "buySell"]) \
                        .agg({"buySell":["count"], "qty":["sum"], "watp":["min", "max"]})
        grouped_df.reset_index(inplace=True)
        grouped_df.columns = ["_".join(i) for i in grouped_df.columns]
        grouped_df_buy = grouped_df[grouped_df["buySell_"] == "BUY"].rename(columns={"symbol_": "symbol",
                                                                                     "name_": "name",
                                                                                     "buySell_count": "buy_trades",
                                                                                     "qty_sum": "buy_qty_sum",
                                                                                     "watp_min": "buy_watp_min",
                                                                                     "watp_max": "buy_watp_max"})
        grouped_df_sell = grouped_df[grouped_df["buySell_"] == "SELL"].rename(columns={"symbol_": "symbol",
                                                                                       "name_": "name",
                                                                                       "buySell_count": "sell_trades",
                                                                                       "qty_sum": "sell_qty_sum",
                                                                                       "watp_min": "sell_watp_min",
                                                                                       "watp_max": "sell_watp_max"})
        merged_df = pd.merge(grouped_df_buy, grouped_df_sell, on=['symbol', 'name'], how='outer')
        merged_df.drop(columns=["buySell__x", "buySell__y"], inplace=True)
        merged_df.reset_index(drop=True, inplace=True)
        return merged_df
    
    def get_daily_fii_dii_data(self):
        # api/fiidiiTradeNse
        # cdsl: 
        #   https://www.cdslindia.com/Publications/ForeignPortInvestor.html
        #   https://www.cdslindia.com/eservices/publications/fiidaily
        # nsdl: https://www.fpi.nsdl.co.in/web/Reports/Latest.aspx
        data = self._get_data("api/fiidiiTradeReact")
        df = pd.DataFrame(data)
        df["category"] = df["category"].apply(lambda x: x.replace("*", "").strip())
        df[["buyValue", "sellValue", "netValue"]] = df[["buyValue", "sellValue", "netValue"]].astype('float64')
        df.rename(columns={"category": "Category", "date": "Date","buyValue": "Buy (₹ Cr)", "sellValue": "Sell (₹ Cr)", "netValue": "Net (₹ Cr)"}, inplace=True)
        return df

    def get_daily_gainers_data(self):
        data = self._get_data("api/live-analysis-variations?index=gainers")
        legends = [i[0] for i in data.get("legends")]
        all_gainers_df_list = []
        for i in legends:
            gainers = data.get(i).get("data")
            gainers_df = pd.DataFrame(gainers)
            gainers_df["legends"] = i
            all_gainers_df_list.append(gainers_df)
        all_gainers_df = pd.concat(all_gainers_df_list)
        all_gainers_df.drop_duplicates(subset=["symbol"], keep='first', inplace=True)
        all_gainers_df.sort_values(axis=0, by=["perChange", "trade_quantity"], ascending=False, inplace=True)
        all_gainers_df.reset_index(drop=True, inplace=True)
        return all_gainers_df

    def get_daily_loosers_data(self):
        data = self._get_data("api/live-analysis-variations?index=loosers")
        legends = [i[0] for i in data.get("legends")]
        all_loosers_df_list = []
        for i in legends:
            loosers = data.get(i).get("data")
            loosers_df = pd.DataFrame(loosers)
            loosers_df["legends"] = i
            all_loosers_df_list.append(loosers_df)
        all_loosers_df = pd.concat(all_loosers_df_list)
        all_loosers_df.drop_duplicates(subset=["symbol"], keep='first', inplace=True)
        all_loosers_df.sort_values(axis=0, by=["perChange", "trade_quantity"], ascending=False, inplace=True)
        all_loosers_df.reset_index(drop=True, inplace=True)
        return all_loosers_df
    
    def get_daily_allIndices_data(self):
        data = self._get_data("api/allIndices")
        df = pd.DataFrame(data.get("data"))
        new_df = df.loc[:, ["index", "open", "high", "low", "last", "advances", "declines", "unchanged", "yearHigh", "yearLow", 
                            "percentChange", "percentChange30d", "percentChange365d"]]
        new_df.columns = [i.capitalize() for i in new_df.columns]
        return new_df

    # https://www.nseindia.com/api/marketStatus
    def get_etf_data(self):
        data = self._get_data(f"api/etf")
        df = pd.DataFrame(data.get("data"))
        return df

    def get_historic_daily_data(self, symbol, from_dt, to_dt, series="EQ"):
        data = self._get_data(f"api/historical/cm/equity?symbol={symbol}&series=[%22{series}%22]&from={from_dt}&to={to_dt}")
        df = pd.DataFrame(data.get("data"))
        return df


if __name__ == "__main__":
    nse_api = NSE_APIS()
    # data = nse_api._get_data("api/snapshot-capital-market-largedeal")
    # data = nse_api.get_historic_daily_data(symbol="BLUESTONE", from_dt="10-09-2024", to_dt="10-09-2025")
    # data = nse_api.get_etf_data()
    # data = nse_api.get_daily_fii_dii_data()
    data = nse_api.get_large_deal_data()
    print(f"{data}")
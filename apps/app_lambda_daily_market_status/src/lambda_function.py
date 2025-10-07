from datetime import datetime as dt
import time
import json
import logging

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

from utils.nse_apis import NSE_APIS
from utils.prepare_market_data import Market_Data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# config details
cur_date = dt.now().strftime("%Y-%m-%d")
dynamodb_region = 'us-east-1'
dynamodb_table_name = 'new_daily_market_data'  # 'daily_market_data'


def load_market_data_into_dynamodb():
    nse_api = NSE_APIS()  

    output = {}
    output["market_date_str"] = cur_date

    multiplier = 10000
    output["market_date_int"] = 0
    for i in cur_date.split("-"):
        output["market_date_int"] = output["market_date_int"] + (int(i) * multiplier)
        multiplier /= 100
    output["market_date_int"] = Decimal(str(output["market_date_int"]))
    output["market_type"] = {}  
    output["market_type"]["daily_large_deal_data"] = json.loads(nse_api.get_daily_large_deal_data().to_json(orient='records'), parse_float=Decimal)
    output["market_type"]["daily_fii_dii_data"] = json.loads(nse_api.get_daily_fii_dii_data().to_json(orient='records'), parse_float=Decimal)
    output["market_type"]["daily_gainers_data"] = json.loads(nse_api.get_daily_gainers_data().to_json(orient='records'), parse_float=Decimal)
    output["market_type"]["daily_loosers_data"] = json.loads(nse_api.get_daily_loosers_data().to_json(orient='records'), parse_float=Decimal)
    output["market_type"]["daily_allIndices_data"] = json.loads(nse_api.get_daily_allIndices_data().to_json(orient='records'), parse_float=Decimal)
    
    # load into dynamodb
    dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
    dynamodb_table = dynamodb.Table(dynamodb_table_name)

    # Store each market type as a separate item
    for market_type_name, market_data in output["market_type"].items():  
        # get table item count
        table_count_response = dynamodb_table.scan(Select='COUNT')
        item_count = table_count_response['ScannedCount']

        # Check if item already exists
        if item_count == 0:
            item = {
                'market_type': market_type_name,
                'market_date': output["market_date_int"],
                'data': market_data,  # This will store the entire dict
                'created_at': dt.utcnow().isoformat(),
                'updated_at': dt.utcnow().isoformat()
            }
            dynamodb_table.put_item(Item=item)
            logger.info(f'Market Data is added for first time for {output["market_date_str"]} - {market_type_name}...., count of items of {dynamodb_table_name}: {item_count+1}')
        elif item_count > 0:
            response = dynamodb_table.get_item(
                Key = {
                    'market_type': market_type_name,
                    'market_date': output["market_date_int"]
                }
            )
            # Ignore insertion If Item exists
            if 'Item' in response:
                logger.info(f'Market Data already exists for {output["market_date_str"]} - {market_type_name}. Skipping..., count of items of {dynamodb_table_name}: {item_count}')
                continue
            else:
                item = {
                    'market_type': market_type_name,
                    'market_date': output["market_date_int"],
                    'data': market_data,  # This will store the entire dict
                    'created_at': dt.utcnow().isoformat(),
                    'updated_at': dt.utcnow().isoformat()
                }
                dynamodb_table.put_item(Item=item)
                logger.info(f'Market Data is added for {output["market_date_str"]} - {market_type_name}...., count of items of {dynamodb_table_name}: {item_count+1}')

def get_market_data_html():
    market_data = Market_Data()
    out = market_data.get_total_item_count()
    print(f"Total Item Count: {out}")

    out = market_data.get_market_data_by_type_date(
        market_type="daily_fii_dii_data",
        market_date_from = 20251006,
        market_date_to = 20251007
    )
    # out.rename(columns={"Buy (₹ Cr)": "Buy_(₹ Cr)", "Sell (₹ Cr)": "Sell_(₹ Cr)", "Net (₹ Cr)": "Net_(₹ Cr)"}, inplace=True)
    out = out[['Date', 'Category', 'Buy (₹ Cr)', 'Sell (₹ Cr)', 'Net (₹ Cr)']]
    print(f"daily_fii_dii_data: \n{out}")

def lambda_handler(event, context):
    # load_market_data_into_dynamodb()
    get_market_data_html()
    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }


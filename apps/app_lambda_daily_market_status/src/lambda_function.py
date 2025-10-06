from datetime import datetime as dt
import time
import json
import logging
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

from utils.nse_apis import NSE_APIS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# config details
cur_date = dt.now().strftime("%Y-%m-%d")
dynamodb_region = 'us-east-1'
dynamodb_table_name = 'new_daily_market_data'  # 'daily_market_data'

# Initialize the TypeDeserializer
deserializer = TypeDeserializer()

def convert_decimal_values(data):
    """
    Recursively converts Decimal objects within a dictionary or list
    to float or int.
    """
    if isinstance(data, dict):
        return {k: convert_decimal_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_values(item) for item in data]
    elif isinstance(data, Decimal):
        # Convert to int if it's a whole number, otherwise to float
        if data % 1 == 0:
            return int(data)
        else:
            return float(data)
    else:
        return data


def deserialize_dynamodb_item(dynamodb_item):
    """
    Deserializes a DynamoDB item (nested dictionary with type indicators)
    into a standard Python dictionary.
    """
    # return {k: deserializer.deserialize(v) for k, v in dynamodb_item.items()}
    return {k: convert_decimal_values(v) for k, v in dynamodb_item.items()}


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


def get_market_data_from_dynamodb():
    items = []
    dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
    dynamodb_table = dynamodb.Table("daily_market_data")

    # Get the latest item if market_date is null 
    # **it isn't fetching latest market_date as it is string
    # response = dynamodb_table.scan()
    # response = dynamodb_table.get_item(key={'market_date': cur_date})

    # get table item count
    table_count_response = dynamodb_table.scan(Select='COUNT')
    item_count = table_count_response['ScannedCount']
    print(f"item_count for daily_market_data: {item_count}")

    market_dates = ["18-09-2025", "19-09-2025", "20-09-2025", "23-09-2025", "24-09-2025", "25-09-2025", "26-09-2025",
                    "27-09-2025", "30-09-2025", "02-10-2025", "03-10-2025", "04-10-2025"]
    market_type = "daily_fii_dii_data"
    history_fii_dii_data_list = []
    try:
        for market_date in market_dates:
            logger.info(f"get market data for date: {market_date} - market_type: {market_type}")
            response = dynamodb_table.query(
                KeyConditionExpression = (
                    Key("market_date").eq(market_date)
                    & Key("market_type").eq(market_type)
                )
            ) 
            history_fii_dii_data = response['Items']
            for i in history_fii_dii_data:
                # history_fii_dii_data_dict = dict()
                # history_fii_dii_data_dict[i.get("market_date")] = deserialize_dynamodb_item(i.get("data")[0])
                history_fii_dii_data_dict = deserialize_dynamodb_item(i.get("data")[0])
            history_fii_dii_data_list.append(history_fii_dii_data_dict)

    except Exception as err:
        logger.error("Couldn't query for %s and market: %s. Here's why: %s",market_date,market_type,err)
        raise
    
    # if items:
    #     latest_item = max(items, key=lambda x: x['market_date'])
    #     return latest_item
    return history_fii_dii_data_list


def lambda_handler(event, context):
    load_market_data_into_dynamodb()

    # history_fii_dii_data = get_market_data_from_dynamodb()
    # logger.info(f"history_fii_dii_data: {history_fii_dii_data}")

    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }


from datetime import datetime as dt
import time
import json
import logging
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

from nse_apis import NSE_APIS

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# config details
cur_date = dt.now().strftime("%d-%m-%Y")
dynamodb_region = 'us-east-1'
dynamodb_table_name = 'daily_market_data'

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
    output["market_date"] = cur_date
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
        # Check if item already exists
        response = dynamodb_table.get_item(
            Key={
                'market_date': output["market_date"],
                'market_type': market_type_name
            }
        )

        # Item exists
        if 'Item' in response:
            logger.info(f'Market Data already exists for {output["market_date"]} - {market_type_name}. Skipping...')
            continue
        else:
            item = {
                'market_date': output["market_date"],
                'market_type': market_type_name,
                'data': market_data,  # This will store the entire dict
                'created_at': dt.utcnow().isoformat(),
                'updated_at': dt.utcnow().isoformat()
            }
            dynamodb_table.put_item(Item=item)
            logger.info(f'Market Data is added for {output["market_date"]} - {market_type_name}....')


def get_market_data_from_dynamodb():
    items = []
    dynamodb = boto3.resource('dynamodb', region_name=dynamodb_region)
    dynamodb_table = dynamodb.Table(dynamodb_table_name)

    # Get the latest item if market_date is null 
    # **it isn't fetching latest market_date as it is string
    # response = dynamodb_table.scan()
    # response = dynamodb_table.get_item(key={'market_date': cur_date})

    market_date_from = "18-09-2025"
    market_date_to = "02-10-2025"
    market_type = "daily_fii_dii_data"
    history_fii_dii_data_list = []
    try:
        logger.info(f"get market data from date: {market_date_from}, to date: {market_date_to} for market_type: {market_type}")
        response = dynamodb_table.query(
            KeyConditionExpression = (
                Key("market_date").GE(market_date_from)
                & Key("market_date").le(market_date_to)
                & Key("market_type").eq(market_type)
            )
        )
    except Exception as err:
        logger.error("Couldn't query for from: %s to: %s and market: %s. Here's why: %s",market_date_from,market_date_to,market_type,err)
        raise
    
    history_fii_dii_data = response['Items']
    for i in history_fii_dii_data:
        history_fii_dii_data_dict = dict()
        history_fii_dii_data_dict[i.get("market_date")] = deserialize_dynamodb_item(i.get("data")[0])
        history_fii_dii_data_list.append(history_fii_dii_data_dict)
    
    # if items:
    #     latest_item = max(items, key=lambda x: x['market_date'])
    #     return latest_item
    return history_fii_dii_data_list


def lambda_handler(event, context):
    # load_market_data_into_dynamodb()

    history_fii_dii_data = get_market_data_from_dynamodb()
    logger.info(f"history_fii_dii_data: {history_fii_dii_data}")
    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }


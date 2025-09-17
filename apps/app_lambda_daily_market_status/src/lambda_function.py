from datetime import datetime as dt
import time
import json
from decimal import Decimal
import boto3
from nse_apis import NSE_APIS


dynamodb_table_name = 'daily_market_data'
cur_date = dt.now().strftime("%d-%m-%Y")

def load_data_into_dynamodb():
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
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table_name)
    
    # Store each market type as a separate item
    for market_type_name, market_data in output["market_type"].items():        
        # Check if item already exists
        response = table.get_item(
            Key={
                'market_date': output["market_date"],
                'market_type': market_type_name
            }
        )

        # Item exists
        if 'Item' in response:
            print(f'Data already exists for {output["market_date"]} - {market_type_name}. Skipping...')
            continue
        else:
            item = {
                'market_date': output["market_date"],
                'market_type': market_type_name,
                'data': market_data,  # This will store the entire dict
                'created_at': dt.utcnow().isoformat(),
                'updated_at': dt.utcnow().isoformat()
            }
            table.put_item(Item=item)
            print(f'Data is added for {output["market_date"]} - {market_type_name}....')

def get_data_from_dynamodb():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table_name)

    # Get the latest item
    response = table.scan()
    items = response['Items']
    if items:
        latest_item = max(items, key=lambda x: x['market_date'])
        return latest_item
    else:
        return None

def lambda_handler(event, context):
    load_data_into_dynamodb()
    # sample_data = get_data_from_dynamodb()
    return {
        'statusCode': 200,
        'body': json.dumps("success")
    }


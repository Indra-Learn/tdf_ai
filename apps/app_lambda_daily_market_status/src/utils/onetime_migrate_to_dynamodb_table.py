# onetime_migrate_to_dynamodb_table.py

from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key


dynamodb_region = 'us-east-1'
dynamodb_table_name_old = 'daily_market_data'
dynamodb_table_name_new = 'new_daily_market_data'

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
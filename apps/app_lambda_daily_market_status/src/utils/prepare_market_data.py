import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from boto3.dynamodb.types import TypeDeserializer


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


class Market_Data:
    dynamodb_region = 'us-east-1'
    dynamodb_table = 'new_daily_market_data'

    def __init__(self):
        dynamodb = boto3.resource('dynamodb', region_name=Market_Data.dynamodb_region)
        self.dynamodb_table = dynamodb.Table(Market_Data.dynamodb_table)

    def get_total_item_count(self):
        table_count_response = self.dynamodb_table.scan(Select='COUNT')
        item_count = table_count_response['ScannedCount']
        return item_count

    def get_market_data_by_type_date(self, market_type, market_date_from, market_date_to):
        out = []
        response = self.dynamodb_table.query(
            KeyConditionExpression = (
                Key('market_type').eq(market_type) 
                & Key('market_date').between(market_date_from, market_date_to)
            )
        )
        for item in response['Items']:
            for record in item['data']:
                out.append(deserialize_dynamodb_item(record))
        out_df = pd.DataFrame(out)
        return out_df
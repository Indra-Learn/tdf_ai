# onetime_migrate_to_dynamodb_table.py

from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer

dynamodb_region = 'us-east-1'
dynamodb_table_name_old = 'daily_market_data'
dynamodb_table_name_new = 'new_daily_market_data'


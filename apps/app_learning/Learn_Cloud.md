# Learn Cloud

## Clouds -

| ID  | Product Name | Billing | Product Details | Docs | Community |
| --- | ------------ | ------- | --------------- | ---- | --------- |
| 1. | [AWS](https://us-east-1.console.aws.amazon.com/) | [Billing]() | [AWS Products](https://aws.amazon.com/campaigns/aws-cloudserver/)</br>EC2, S3, Lambda Func, Dynamodb etc. | [Docs](https://docs.aws.amazon.com/) | [Community](https://repost.aws/)
| 2.  | [GCP]() | [Billing]() | [GCP Products]() | [Docs]() | [Community]()
| 3.  | [MongoDB](https://cloud.mongodb.com/) | [Billing]() | Atlas | [Docs]() | [Community]()


## Services -

| ID  | Service Name | Type | Pricing | Details | Docs |
| --- | ------------ | ---- | ------- | ------- | ---- |
| 1. | [Lambda Function]() | Data Pipeline | [Pricing]() | 1. Serverless pipeline | [Docs]() |
| 2.  | [DynamoDB]() | Database | [Pricing](https://aws.amazon.com/dynamodb/pricing/) | 1. NoSQL, Schemaless, fully managed database | 1. [Docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html) </br> 2. [Cheat Sheet](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CheatSheet.html) </br> 3. [Python SDK Sample](https://docs.aws.amazon.com/code-library/latest/ug/python_3_dynamodb_code_examples.html) |


## Extras -
--=================================== </br>
-- AWS Lambda Function (fetch_store_daily_market_data) </br>
--=================================== </br>
1. Amazon Lambda Function
2. Function name: fetch_store_daily_market_data
3. Runtime: Python 3.13
4. Architecture: x86_64
5. Lambda will create an execution role with permissions to upload logs to Amazon CloudWatch Logs
6. Create **Layer** to install/add python packages
7. after creating aws lambda function, click "add trigger", 
    1. Trigger configuration: EventBridge
    2. Rule: Create new rule
    3. rule name: 
    4. rule type: Schedule expression
7. after creating aws lambda function, go to "Configuration" and change "Timeout" to 30 sec. 


--=================================== </br>
-- Amazon Dynamodb (daily_market_data) </br>
--=================================== </br>
1. Table name: daily_market_data
2. Partition key: market_date, string 
3. Sort key - optional: market_type, string
4. Table settings: Customize settings
5. Table class: DynamoDB Standard-IA
6. Read/write capacity settings: Capacity mode: On-demand
7. Encryption key management: AWS owned key
8. 


## Questions based on practical implementation:

1. What is Layer in Lambda function?
2. What is amazondynamodbfullaccess vs amazondynamodbfullaccess v2?
2. How to implement "idempotent" or handle not to insert same/duplicate data if same job has been triggered multiple times by mistake. Why we need to check Item_count should be greater than 0 to check there is already exist of same record?
3. What is "M", "L", "B", "S" keys in dynamodb table?
4. Difference between boto3.resource('dynamodb').Table(dynamodb_table_name).scan and query?
4. What is the differene between "client" and "resource" in boto3?
5. What is "*" and "?" in AWS EventBridge Cron syntax? 
6. **Why You cannot specify both day-of-month and day-of-week in the same cron expression, so one must be "?" ?
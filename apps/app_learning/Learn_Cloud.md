# Learn Cloud

## Clouds -

| ID  | Product Name | Billing | Product Details | Docs | Community |
| --- | ------------ | ------- | --------------- | ---- | --------- |
| 1. | [AWS](https://us-east-1.console.aws.amazon.com/) | [Billing]() | [AWS Products](https://aws.amazon.com/campaigns/aws-cloudserver/)</br>EC2, S3, Lambda Func, Dynamodb etc. | [Docs](https://docs.aws.amazon.com/) | [Community](https://repost.aws/)
| 2.  | [MongoDB](https://cloud.mongodb.com/) | [Billing]() | Atlas | [Docs]() | [Community]()


## Services -

| ID  | Service Name | Type | Pricing | Details | Docs |
| --- | ------------ | ---- | ------- | ------- | ---- |
| 1. | [Lambda Function]() | Data Pipeline | [Pricing]() | 1. Serverless pipeline | [Docs]() |
| 2.  | [DynamoDB]() | Database | [Pricing](https://aws.amazon.com/dynamodb/pricing/) | 1. NoSQL, fully managed database | [Docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html) |


## Extras -
--============================================</br>
-- AWS Lambda Function (fetch_store_daily_market_data)</br>
--============================================</br>
1. Amazon Lambda Function
2. Function name: fetch_store_daily_market_data
3. Runtime: Python 3.13
4. Architecture: x86_64
5. Lambda will create an execution role with permissions to upload logs to Amazon CloudWatch Logs
6. Create **Layer**


--============================================</br>
-- Amazon Dynamodb (daily_market_data)</br>
--============================================</br>
1. Table name: daily_market_data
2. Partition key: market_date, string 
3. Sort key - optional: market_type, string
4. Table settings: Customize settings
5. Table class: DynamoDB Standard-IA
6. Read/write capacity settings: Capacity mode: On-demand
7. Encryption key management: AWS owned key
8. 


1. What is Layer in Lambda function?
2. What is amazondynamodbfullaccess vs amazondynamodbfullaccess v2?
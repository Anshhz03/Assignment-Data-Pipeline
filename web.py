import requests
import json
import csv
import boto3
import time

def lambda_handler(event, context):
    # Load secret from Secrets Manager
    secret_name = "alphavantage_key"
    region_name = "us-east-1"

    session = boto3.session.Session()
    secrets_client = session.client(service_name='secretsmanager', region_name=region_name)
    secret_response = secrets_client.get_secret_value(SecretId=secret_name)
    secret = json.loads(secret_response['SecretString'])
    AlphaVantage_api_key = secret['AlphaVantageAPIKey']

    # Create api.json inside Lambda
    config_data = {
        "coingecko": {
            "url": "https://api.coingecko.com/api/v3/coins/markets",
            "params": {
                "vs_currency": "usd",
                "ids": "bitcoin,ethereum"
            }
        },
        "alphavantage": {
            "url": "https://www.alphavantage.co/query",
            "params": {
                "function": "TIME_SERIES_DAILY",
                "symbol": "IBM"
            }
        }
    }

    s3 = boto3.client('s3')
    bucket_name = 'cg-test-bucket-00'

    # Uploading api.json to S3 bucket 
    s3.put_object(
        Bucket=bucket_name,
        Key='api.json',
        Body=json.dumps(config_data, indent=4),
        ContentType='application/json'
    )

    
    config_obj = s3.get_object(Bucket=bucket_name, Key='api.json')
    config_data = json.loads(config_obj['Body'].read().decode('utf-8'))

    headers = {"User-Agent": "Mozilla/5.0"}

    
    cg_response = requests.get(
        config_data['coingecko']['url'],
        params=config_data['coingecko']['params'],
        headers=headers
    )
    coin_data = cg_response.json()

    
    s3.put_object(
        Bucket=bucket_name,
        Key='coingecko_data.json',
        Body=json.dumps(coin_data, indent=4),
        ContentType='application/json'
    )

    time.sleep(12)  # AlphaVantage rate limit

    
    av_params = config_data['alphavantage']['params']
    av_params['apikey'] = AlphaVantage_api_key

    av_response = requests.get(
        config_data['alphavantage']['url'],
        params=av_params,
        headers=headers
    )
    av_data = av_response.json()
    time_series = av_data.get("Time Series (Daily)", {})

    # Convert to CSV
    csv_content = "Date,Open,High,Low,Close,Volume\n"
    for date, values in time_series.items():
        csv_content += f"{date},{values.get('1. open','')},{values.get('2. high','')},{values.get('3. low','')},{values.get('4. close','')},{values.get('5. volume','')}\n"

    # Upload directly
    s3.put_object(
        Bucket=bucket_name,
        Key='alphavantage_data.csv',
        Body=csv_content,
        ContentType='text/csv'
    )

    return {"status": "success"}
# Data Engineering Assignment
This project builds a full-fledged data pipeline that pulls crypto and stock details from the CoinGecko and Alphavantage respectively, processes the information, and stores it in Snowflake. It leverages several AWS services—including Lambda, S3, SNS, —to automate and streamline the entire workflow 


## 1. Data Extraction with AWS Lambda
A Python script running on an AWS Lambda function is scheduled to fetch crypto a stocks data from the CoinGecko (https://api.coingecko.com/api/v3/coins/markets) And Alphavantage (https://www.alphavantage.co/query"), respectively. The Lambda function extracts relevant crypto and stock metadata
<img width="946" height="393" alt="image" src="https://github.com/user-attachments/assets/c5c50eba-e38a-4b03-8ea7-0c04d320b72b" />


## 2. Storing Extracted Data in Amazon S3 Bucket
The extracted data for Alphavantage is saved as CSV file and Coingecko is saved as JSON file in an Amazon S3 bucket. 
<img width="946" height="390" alt="image" src="https://github.com/user-attachments/assets/3ca2e7f3-9f1e-48a3-b190-10fe7eeb6a7a" />


## 3. Ingesting Data into Snowflake via Snowpipe
With the help of Snowpipe, data stored in an S3 bucket is automatically loaded into a Snowflake table called rawDataCongecko and raw_dataAlpha. This seamless ingestion is made possible by setting up S3 event notifications, which trigger Amazon SNS (Simple Notification Service).
<img width="1014" height="296" alt="image" src="https://github.com/user-attachments/assets/5aea7971-0c87-48d0-8462-2da1da5cd8fe" />
<img width="946" height="392" alt="image" src="https://github.com/user-attachments/assets/cd37018f-88ac-4a2a-a653-dade99a2f22c" />



## 4. Conversion of JSON to CSV
Coingecko’s data was extracted in JSON form and it was transformed to CSV by flattening and parsing. 
<img width="946" height="407" alt="image" src="https://github.com/user-attachments/assets/c012793c-72ac-4c7a-9c6a-5c20618d265b" />

## 5. Removing Duplicate Values
Duplicate Values were removed from the dataset using DISTINCT query by  wrapping the SELECT with DISTINCT. This ensures that if the CROSS JOIN or JOIN ON 1=1 creates repeated rows, they will get eliminated.
<img width="946" height="455" alt="image" src="https://github.com/user-attachments/assets/efb6f0d8-7e06-42ef-9bd4-55bcdda47d5c" />

## 6. Creating Views for the Datasets
Two Views were created for both the datasets.
<img width="946" height="464" alt="image" src="https://github.com/user-attachments/assets/21e79dab-6600-49fb-8b6f-64efbed35d2b" />
<img width="946" height="464" alt="image" src="https://github.com/user-attachments/assets/027964b4-3f4a-4bff-baeb-714f401d3532" />










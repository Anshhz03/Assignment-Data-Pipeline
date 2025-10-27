# Data Engineering Assignment
This project builds a full-fledged data pipeline that pulls book details, processes the information, and stores it in Snowflake. It leverages several AWS services—including Lambda, S3, SNS, and SQS—to automate and streamline the entire workflow


## 1. Data Extraction with AWS Lambda
A Python script running on an AWS Lambda function is scheduled to periodically fetch books data from the http://books.toscrape.com/catalogue/page-{}.html
The Lambda function extracts relevant Book metadata such as:
Title
Price
<img width="1363" height="616" alt="Screenshot 2025-10-27 125700" src="https://github.com/user-attachments/assets/5566346b-2929-4d35-8671-a6de13b4174f" />

## 2. Storing Extracted Data in Amazon S3 Bucket
The extracted data is saved as CSV files in an Amazon S3 bucket. 
<img width="1334" height="606" alt="image" src="https://github.com/user-attachments/assets/387bd060-e52e-4940-9aba-8acd6b8c6851" />

## 3. Ingesting Data into Snowflake via Snowpipe
With the help of Snowpipe, data stored in an S3 bucket is automatically loaded into a Snowflake table called raw_data. This seamless ingestion is made possible by setting up S3 event notifications, which trigger Amazon SNS (Simple Notification Service) and Amazon SQS (Simple Queue Service) to coordinate and manage the data flow efficiently.
<img width="997" height="552" alt="image" src="https://github.com/user-attachments/assets/74e34403-3df2-4f16-8775-1d1ed85b3f5b" />

## 4. Data Transformation Using Streams and Tasks
A Snowflake Stream is created on the raw_data table to capture changes (inserts/updates).
A Snowflake Task is scheduled to run a MERGE operation that:
Standardizes the Overview field
Categorizes movies based on Price as Premium and Standard
Inserts or updates records in the transformed_data table
<img width="1299" height="616" alt="image" src="https://github.com/user-attachments/assets/59d62568-6637-472a-95e3-5b5f2a31a450" />

<img width="1293" height="644" alt="image" src="https://github.com/user-attachments/assets/816bbbc4-158b-4f1e-b0de-660a689a536d" />

<img width="1299" height="648" alt="image" src="https://github.com/user-attachments/assets/118928f4-bc29-4835-be14-03a4a0c21788" />







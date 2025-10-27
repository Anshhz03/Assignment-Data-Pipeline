import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3

def lambda_handler(event=None, context=None):
    
    base_url = "http://books.toscrape.com/catalogue/page-{}.html"

    titles = []
    prices = []

    
    for page in range(1, 6):
        url = base_url.format(page)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        books = soup.find_all('article', class_='product_pod')

        for book in books:
            title = book.h3.a['title']
            price = book.find('p', class_='price_color').text

            titles.append(title)
            prices.append(price)

    
    df = pd.DataFrame({
        'Title': titles,
        'Price': prices
    })
    
    df['Price'] = df['Price'].str.replace('Â£', '').astype(float)


    local_path = "/tmp/books.csv"
    df.to_csv(local_path, index=False)
    print("Scraped data saved to /tmp/books.csv")

    # Upload to S3
    s3 = boto3.client('s3')
    bucket_name = 'cg-test-bucket-00'  
    s3_key = 'books.csv'

    s3.upload_file(local_path, bucket_name, s3_key)
    print(f"{s3_key} uploaded to S3 bucket {bucket_name}")

    return {
        'statusCode': 200,
        'body': f"File uploaded to S3: {bucket_name}/{s3_key}"
    }
import json, requests, os
from datetime import timedelta, datetime
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

# I ran the initial script with start_year parameter to fetch historical data, however, subsequent runs runs with the today parameter
today = datetime.now().strftime('%Y-%m-%d')
start_year = '2024'
order_by = 'date'
api_key = Variable.get('NETFLIX_API_KEY')  # New environment variable for API key
limit = 200 

def extract_titles():
    all_data = []
    offset = 0

    # Continue fetching data until there are no more results left
    while True:
        url = f"https://api.apilayer.com/unogs/search/titles?start_year={start_year}&order_by={order_by}&limit={limit}&offset={offset}"
        payload = {}
        headers = {"apikey": api_key}

        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()

        # Checking if there are no more results left
        if not data['results']:
            break
            print(f'No new movie released today{ today}')
       
        for result in data["results"]:
            all_data.append({
                "imdb_id": result["imdb_id"],
                "title": result["title"],
                "rating": result["rating"],
                "year": result["year"],
                "runtime": result["runtime"],
                "top250": result["top250"],
                "top250tv": result["top250tv"],
                "title_date": result["title_date"]
            })

        # Increment the offset for the next request
        offset += limit
        print (f'loding {offset} data')

    
    df = pd.DataFrame(all_data)

    # Saving the DataFrame as a new CSV file with timestamp to make each file name unique
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = f'Netflix2024_{timestamp}.csv'
    print(f"CSV file {csv_file_name} saved successfully.")
    df.to_csv(csv_file_name, index=False)
   
    return csv_file_name




def upload_titles_to_s3(**kwargs):
    # Retrieve the extracted file path from XCom
    extracted_file_path = kwargs['ti'].xcom_pull(task_ids='extract_movies_task')

    # Initialize S3Hook
    s3_hook = S3Hook(aws_conn_id='aws_connect')  # Assuming you have set up a connection in Airflow for S3

    # Upload the file to S3
    s3_bucket = 'myde-tf-bucket'
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_key = f'titles/Netflix_movie{timestamp}.csv'  # Specify the destination path in S3
    s3_hook.load_file(filename=extracted_file_path, key=s3_key, bucket_name=s3_bucket)
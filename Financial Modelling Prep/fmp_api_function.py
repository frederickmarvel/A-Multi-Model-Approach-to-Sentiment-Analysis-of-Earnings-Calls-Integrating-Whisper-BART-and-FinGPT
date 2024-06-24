import time
import pandas as pd
import requests
import csv

fmp_api_token = #rahasia bosq

def requests_get(url):
    try:
        response = requests.get(url)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Request failed with status code {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def get_batch_earning_calls(stock_name, year):
    url = f"https://financialmodelingprep.com/api/v4/batch_earning_call_transcript/{stock_name}?year={year}&apikey={fmp_api_token}"
    result = requests_get(url)
    return result

def process_dataset(input_filename, output_filename):
    with open(input_filename, mode='r') as infile, open(output_filename, mode='w', newline='') as outfile:
        reader = csv.DictReader(infile)
        fieldnames = ['Exchange', 'Symbol', 'Year', 'EarningCallTranscript']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        request_count = 0
        start_time = time.time()
        
        for row in reader:
            symbol = row['Symbol']
            for year in range(2021, 2024):
                result = get_batch_earning_calls(symbol, year)
                request_count += 1
                
                if result:
                    for call in result:
                        writer.writerow({'Exchange': row['Exchange'], 'Symbol': symbol, 'Year': year, 'EarningCallTranscript': call.get('content', '')})
                else:
                    print(f"No data for {symbol} in {year}")
                
                # Check if we need to wait to avoid exceeding 100 requests per minute
                if request_count >= 95:
                    elapsed_time = time.time() - start_time
                    if elapsed_time < 60:
                        time_to_sleep = 60 - elapsed_time
                        print(f"Sleeping for {time_to_sleep} seconds to avoid exceeding rate limit")
                        time.sleep(time_to_sleep)
                    request_count = 0
                    start_time = time.time()

input_filename = "/root/skripsi/FMP/sp500_companies.csv"
output_filename = "earning_calls_transcript_1k.csv"
process_dataset(input_filename, output_filename)

from pprint import pprint
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from google.cloud import storage
from fake_useragent import UserAgent

def upload_to_bucket(destination_path, destination_blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        './key.json')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_path+destination_blob_name)
    blob.upload_from_filename(path_to_file)
    return blob.public_url

user_agent = UserAgent()

base_url = "https://www.coingecko.com/en/coins/"

coins = ["bitcoin","ethereum","tether"]

tables = []

while 1:

    yr, month, day, hr, minute , sec = map(int, time.strftime("%Y %m %d %H %M %S").split())

    round=1;

    for coin in coins:
        try:
            headers= {
                'User-Agent': user_agent.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            tmp_url = base_url + coin
            print(tmp_url)
            response =requests.get(tmp_url, headers=headers)
            print(response)
            soup = BeautifulSoup(response.content, 'html.parser')
            res = pd.read_html(str(soup))[0]
            res = res.T
            res = res.iloc[1: , :]
            print(res)

        #    master_table = pd.concat(res)
        #    print(res)
        #    res.loc[:,res.columns[1:-1]]
            file_name = coin + f'_{yr}_{month}_{day}_{hr}_{minute}_{sec}.{round}.csv'
            res.to_csv(file_name, index=False)
            upload_to_bucket("Nhom_16/crypto_data_v4/" + coin + "/", file_name, "./" + file_name, "bigdata-class-2023") 
        except Exception as err:
            print(err)
            f = open("err.txt", "a")
            f.write(f"there are something wrong on {coin} in {yr}_{month}_{day}_{hr}_{minute}_{sec}\n")
            f.close()
    time.sleep(20)    

    round= round + 1;


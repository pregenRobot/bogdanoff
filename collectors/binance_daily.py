import json
import pandas as pd
import requests
from requests.exceptions import HTTPError
import subprocess
import os
from bs4 import BeautifulSoup as Soup
import time
import shutup

shutup.please()

def create_fetch_request(path:str):
    curl = f"""

    curl 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix={path}' \
      -H 'Connection: keep-alive' \
      -H 'sec-ch-ua: " Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"' \
      -H 'Accept: */*' \
      -H 'sec-ch-ua-mobile: ?0' \
      -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36' \
      -H 'Origin: https://data.binance.vision' \
      -H 'Sec-Fetch-Site: cross-site' \
      -H 'Sec-Fetch-Mode: cors' \
      -H 'Sec-Fetch-Dest: empty' \
      -H 'Referer: https://data.binance.vision/' \
      -H 'Accept-Language: en-GB,en-US;q=0.9,en;q=0.8' \
      --compressed

    """
    return curl

def exec_curl_request(curl_request:str) -> str:
    time.sleep(1)
    curl_execution = subprocess.run(curl_request, shell=True, stdout=subprocess.PIPE).stdout
    return curl_execution.decode("utf-8")

def get_subdirs(html: str,selector:str="Prefix"):
    soup = Soup(html)
    return list(map(lambda node: node.getText() ,soup.select(selector)[1:]))

def fetch_dir(root_dir: str, selector:str="Prefix") -> "list[str]":
    curl_request = create_fetch_request(root_dir)
    response = exec_curl_request(curl_request)
    subdirs = get_subdirs(response, selector)
    return subdirs

try:
    # print(fetch_dir("data/spot/daily/aggTrades/ADAAUD/", "Key"))
    for methodology in fetch_dir("data/spot/daily/","Prefix"):
        print(methodology)
        for ticker in fetch_dir(methodology, "Prefix"):
            print(ticker)
            with open("../../dumps/urls.txt", "a+") as f:
                f.write("\n".join(fetch_dir(ticker,"Key")))

except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')  # Python 3.6
except Exception as err:
    print(f'Other error occurred: {err}')  # Python 3.6
else:
    print('Success!')



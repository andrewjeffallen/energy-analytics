import requests
import json
import time
import numpy as np
import pandas as pd
import boto3

import io
from pandas import json_normalize
import json, requests, urllib, io

from pandas.io.json import json_normalize

import numpy 
import pandas as pd
import os
import json
import boto3
import io
import gzip
import sys
from datetime import date

token="INSERT_TOKEN_HERE"


# Retrieve Authorizations

def get_authorization_list():
    authorization_list = []
    url ='https://utilityapi.com/api/v2/authorizations'
    headers = {
        'Authorization': 'Bearer {token}',
        'Content-Type': 'application/json'
    }
    r = requests.get(url,
                     headers=headers
                    )
    json.loads(r.text)['authorizations']
    
    for i in range(-1,len(json.loads(r.text)['authorizations'])-1):
        authorization_list.append(json.loads(r.text)['authorizations'][i]['uid'])

    return authorization_list

# Retrieve Active Meters

def get_active_meters():
    url = 'https://utilityapi.com/api/v2/meters'
    headers = {
        'Authorization': 'Bearer {token}',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    download = json.loads(r.text)
    
    active_meters=[]
    
    for i in range(len(download['meters'])):
        if download['meters'][i]['is_activated']==True:
            active_meters.append(download['meters'][i]['uid'])
    return active_meters


# Retrieve historical bills in Dataframe given meter_uid
            
def get_bills(meter_uid):
    url =f'https://utilityapi.com/api/v2/files/meters_bills_csv?meters={meter_uid}'
    headers = {
        'Authorization': 'Bearer {token}',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)


# send bills dataframe to S3

def send_bills_to_s3(meter_uid):
    df=get_bills(meter_uid).iloc[:,:17]
    
    print(f'Loading {len(df)} Rows to S3 for meter_uid {meter_uid}')
    
    load_date = date.today().strftime("%Y-%m-%d")
    print("Load Date:", load_date)
    
    session = boto3.session.Session(profile_name="data-arch", )
    s3_client = session.client("s3", use_ssl=False)    

    csv_buffer = io.StringIO()
    
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    gz_buffer = io.BytesIO()
    

    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
    try:
        s3_client.put_object(Bucket='utility-api', Key=f"""bills/{load_date}/{meter_uid}.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code



# Retrieve historical intervals in Dataframe given meter_uid

def get_intervals(meter_uid):
    url = f'https://utilityapi.com/api/v2/files/intervals_csv?meters={meter_uid}'
    headers = {
        'Authorization': 'Bearer {token}',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)


# Send intervals dataframe to s3 

def send_intervals_to_s3(meter_uid):
    df = get_intervals(meter_uid)
    df.astype = {
        'meter_uid':int, 
        'utility':str, 
        'utility_service_id':int, 
        'utility_service_address':str,
        'utility_meter_number':int, 
        'utility_tariff_name':str, 
        'interval_start':str,
        'interval_end':str, 
        'interval_kWh':int, 
        'net_kWh':int, 
        'source':str, 
        'updated':str,
        'interval_timezone':str
    }
    
    print(f'Loading {len(df)} Rows to S3 for meter_uid {meter_uid}')
    
    load_date = date.today().strftime("%Y-%m-%d")
    print("Load Date:", load_date)
    
    session = boto3.session.Session(profile_name="data-arch", )
    s3_client = session.client("s3", use_ssl=False)    

    csv_buffer = io.StringIO()
    
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    gz_buffer = io.BytesIO()
    

    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
    try:
        s3_client.put_object(Bucket='utility-api', Key=f"""intervals/historical/{load_date}/meter_uid_{meter_uid}_intervals.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code



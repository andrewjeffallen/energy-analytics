import requests
import json
import time
import numpy as np
import pandas as pd
import boto3

from pandas.io.json import json_normalize

import numpy 
import pandas as pd
import os
import json
import boto3
import io
import gzip
from geopy.geocoders import Nominatim
import sys
from datetime import date
import json, requests, urllib, io



def get_active_meters(token):
    url = 'https://utilityapi.com/api/v2/meters'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    download = json.loads(r.text)
    
    active_meters=[]
    
    for i in range(len(download['meters'])):
        if download['meters'][i]['is_activated']==True:
            active_meters.append(download['meters'][i]['uid'])
    return active_meters

# INTERVALS
def get_intervals(meter_uid,token):
    url = f'https://utilityapi.com/api/v2/files/intervals_csv?meters={meter_uid}'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)

def send_intervals_to_s3(meter_uid,token):
    df=get_intervals(meter_uid,token)
    
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
        s3_client.put_object(Bucket='utility-api', Key=f"""intervals/{meter_uid}/{load_date}.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code

# BILLS
def get_bills(meter_uid,token):
    url =f'https://utilityapi.com/api/v2/files/meters_bills_csv?meters={meter_uid}'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)


def send_bills_to_s3(meter_uid,token):
    df=get_bills(meter_uid,token)
    
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
        s3_client.put_object(Bucket='utility-api', Key=f"""bills/{meter_uid}/{load_date}.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code


if __name__ == '__main__':
    meter_uid=sys.argv[1]
    meter_file=sys.argv[2]
    token=sys.argv[3]
    
    if meter_file = 'bills':
        return_code=send_bills_to_s3(meter_uid,token)
        if return_code == 0:
            sys.exit(0)
        else:
            raise SystemError(f"Error {return_code}")
    elif meter_file = 'intervals':
        return_code=send_intervals_to_s3(meter_uid,token)
        if return_code == 0:
            sys.exit(0)
        else:
            raise SystemError(f"Error {return_code}")
            
            
    
    
    
    

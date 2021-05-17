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

def get_active_meters():
    url = 'https://utilityapi.com/api/v2/meters'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    download = json.loads(r.text)
    
    active_meters=[]
    
    for i in range(len(download['meters'])):
        if download['meters'][i]['is_activated']==True and download['meters'][i]['is_archived']==False :
            active_meters.append(download['meters'][i]['uid'])
    return active_meters

def get_bills(meter_uid):
    url =f'https://utilityapi.com/api/v2/files/meters_bills_csv?meters={meter_uid}'
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)



# Test whether a meter bill has `Demand_kw` or not and get list of those meters
def test_demand_kw_in_bills():
    no_demand_kw=[]
    all_active = get_active_meters()
    for i in all_active:
        try:
            get_bills(i)['Demand_kw']
            return_code=0
        except Exception as e:
            return_code=1
#             print(e)
        if return_code==1:
            no_demand_kw.append(i)        
    return no_demand_kw


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

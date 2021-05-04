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

## Works for Valid METER_UID to grab entire meter intervals history:
def get_intervals(meter_uid):
    url = f'https://utilityapi.com/api/v2/files/intervals_csv?meters={meter_uid}'
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    download = requests.get(url, headers=headers).content
    return pd.read_csv(io.StringIO(download.decode('utf-8')), error_bad_lines=False)


def get_authorization_list():
    authorization_list = []
    url ='https://utilityapi.com/api/v2/authorizations'
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    json.loads(r.text)['authorizations']
    
    for i in range(-1,len(json.loads(r.text)['authorizations'])-1):
        authorization_list.append(json.loads(r.text)['authorizations'][i]['uid'])

    return authorization_list



def get_meter_info_from_authorization(authorization_uid):
    url = f'https://utilityapi.com/api/v2/meters?authorizations={authorization_uid}'
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)



def get_meter_list_from_auth(authorization_uid, **context):
    meter_list=[]
    for i in range(-1,len(get_meter_info_from_authorization(authorization_uid)['meters'])):
        meter_list.append(get_meter_info_from_authorization(authorization_uid)['meters'][i]['uid'])
        print(get_meter_info_from_authorization(authorization_uid)['meters'][i]['uid'])
    return pd.DataFrame(meter_list)
            
            
#Collect past intervals from the meter
def get_intervals(meter_uid):
    url = f'https://utilityapi.com/api/v2/intervals?meters={meter_uid}'
    headers = {
        'Authorization': 'Bearer 2793bc2c7aeb4013bf817f656213e056',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)


def send_intervals_to_s3(meter_uid):
    
    raw_int=get_intervals(meter_uid=meter_uid)
    json_dict =get_intervals(meter_uid=meter_uid)['intervals']


    flattened_df= pd.concat(
        [pd.DataFrame(
            json_dict
        ), 
         pd.DataFrame(
             list(
                 json_dict[0]['readings']
             )
         )
        ],
        axis=1) \
            .drop('readings', 1)

    intervals = flattened_df[['start','end','kwh']]
    intervals.loc[:,'meter_uid'] = flattened_df['meter_uid'][1]
    intervals.loc[:,'authorization_uid'] = flattened_df['authorization_uid'][1]
    intervals.loc[:,'utility'] = flattened_df['utility'][1]
    intervals.loc[:,'service_address'] = list(flattened_df['base'])[0]['service_address']
    
    df = intervals
    print(f'Loading {len(df)} Rows to S3 for meter_uid {meter_uid}')
    
    load_date = date.today().strftime("%Y-%m-%d")
    
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

meter_list = [ 725797, 725798, 725799, 725801]

if __name__ == "__main__":

    meter_uid = sys.argv[1]
    return_code = send_intervals_to_s3(meter_uid)

    if return_code == 0:
        sys.exit(0)
    else:
        raise SystemError(f"Error {return_code}")
        


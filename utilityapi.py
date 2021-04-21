import requests
import json
import time
import numpy as np
import pandas as pd


INSERTTOKENHERE=' your utiltiy-api token'

def get_authorization_list():
    authorization_list = []
    url ='https://utilityapi.com/api/v2/authorizations'
    print(url)
    headers = {
        'Authorization': 'Bearer INSERTTOKENHERE',
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
        'Authorization': 'Bearer INSERTTOKENHERE',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)



def get_meter_list_from_auth(authorization_uid, **context):
    print(f"Meters for Authorization {authorization_uid}:")
    meter_list=[]
    for i in range(-1,len(get_meter_info_from_authorization(authorization_uid)['meters'])):
        meter_list.append(get_meter_info_from_authorization(authorization_uid)['meters'][i]['uid'])
        print(get_meter_info_from_authorization(authorization_uid)['meters'][i]['uid'])
    return pd.DataFrame(meter_list)
            
            
#Collect past intervals from the meter
def get_intervals(meter_uid):
    url = f'https://utilityapi.com/api/v2/intervals?meters={meter_uid}'
    print(url)
    headers = {
        'Authorization': 'Bearer INSERTTOKENHERE',
        'Content-Type': 'application/json'
    }
    r = requests.get(url, headers=headers)
    return json.loads(r.text)


def send_csv_gz_to_s3(meter_uid):
    
    json_dict =get_intervals(meter_uid=meter_uid)['intervals']
    df= pd.concat([pd.DataFrame(json_dict), pd.DataFrame(list(json_dict[0]['readings']))], axis=1).drop('readings', 1)
    
    load_date = date.today().strftime("%Y-%m-%d")
    
    session = boto3.session.Session(profile_name="default", )
    s3_client = session.client("s3", use_ssl=False)    

    csv_buffer = io.StringIO()
    
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    gz_buffer = io.BytesIO()
    

    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        gz_file.write(bytes(csv_buffer.getvalue(), 'utf-8'))
    try:
        s3_client.put_object(Bucket='utility-api-raw', Key=f"""intervals/{meter_uid}/{load_date}.csv.gz""", Body=gz_buffer.getvalue())
        return_code = 0
    except Exception as e:
        return_code = 1
        print(e)
    return return_code



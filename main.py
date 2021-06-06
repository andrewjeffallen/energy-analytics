import json, requests, urllib, io
import time
import numpy as np
import pandas as pd
import boto3
import gzip
import sys
import snowflake

import boto3
import base64
from botocore.exceptions import ClientError

from datetime import date
from pandas import json_normalize
from pandas.io.json import json_normalize

from src.aws import *
from src.utilityapi import * 
from src.snowflake import * 


def execute_load_s3_and_copy_into_snowflake(utility_file):
    
    try:
        execute_load_s3()
        return_code_s3=0
    except Exception as e:
        return_code_s3=1
        print(e)
    return return_code_s3

    if return_code_s3==0:
        print("")
        print("Load to S3 Successful...")
        print(f"Beginning load to Snowflake for {utility_file}")
        print("")
        try:
            load_data_to_snowflake(utility_file)
            return_code=0
        except Exception as e:
            return_code=1
            print(e)
        return return_code
    
    if return_code==0:
        print("Load to Snowflake completed successfully")
        
        
        
if __name__ == "__main__":
    f"""
    Function to run GET from utilityapi.com endpoint, stream data to s3 and load into Snowflake
    """

    utility_file = sys.argv[1]
    


    return_code = execute_load_s3_and_copy_into_snowflake(utility_file)

    if return_code == 0:
        sys.exit(0)
    else:
        raise SystemError(f"Error {return_code}")
    
   

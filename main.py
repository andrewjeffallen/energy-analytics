import json, requests, urllib, io
import time
import numpy as np
import pandas as pd
import boto3
import gzip
import sys
import snowflake.connector

import boto3
import base64
from botocore.exceptions import ClientError

from datetime import date
from pandas import json_normalize
from pandas.io.json import json_normalize

from src.aws import *
from src.utilityapi import * 
from src.snowflake import * 


def execute_load_s3_and_copy_into_snowflake():
    
    return_code_s3 = execute_load_s3()
    if return_code_s3 == 0:
        print("")
        print("Load to S3 Succesful..")
        print("Beginning load to Snowflake")
        print(" ")
        return_code_bills = load_data_to_snowflake("bills")
        
        if return_code_bills == 0:
            print("Loaded bills data to Snowflake Succesfully")
        elif return_code_bills == 1:
            print("Error Loading Bills data to Snowflake")
            print("Please Validate")
            print("Exiting process")
            print("")
            
        return_code_ints = load_data_to_snowflake("intervals")
        
        if return_code_ints == 0:
            print("Loaded intervals data to Snowflake Succesfully")
        elif return_code_ints == 1:
            print("Error Loading Intervals data to Snowflake")
            print("Please Validate")
            print("Exiting process")
            print("")
    
                
        
if __name__ == "__main__":
    f"""
    Function to run GET from utilityapi.com endpoint, stream data to s3 and load into Snowflake
    """
    
    execute_load_s3_and_copy_into_snowflake()

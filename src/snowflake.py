import json, requests, urllib, io
import time
import numpy as np
import pandas as pd
import boto3
import gzip
import sys
import snowflake.connector

from datetime import date

import boto3
import base64
from botocore.exceptions import ClientError

from src.aws import *


def snowflake_connection(schema):
    
    creds = get_secret('utility-database-credentials')
    
    con = snowflake.connector.connect(
        user=creds.get("user"),
        password=creds.get("password"),
        account=creds.get("account"),
        warehouse=creds.get("warehouse"),
        database=creds.get("database"),
        schema=schema,
        role=creds.get('role')
        )
    return con

def load_data_to_snowflake(utility_file):
    
    load_date = date.today().strftime("%Y-%m-%d")
    schema='STAGE'
    external_stage='utility_api_stage'
     
    if utility_file=='bills':
        utility_file_length=16
        table='BILLS_RAW_SRC'
        
        print("")
        print(f"loading {utility_file} data for files ingested on {load_date}")
        print("")
        con=snowflake_connection(schema)
        cs=con.cursor()
        print(f"Truncating {schema}.{table} Prior to Load")
        cs.execute(f"""truncate {schema}.{table};""")
        print(f"Succesfully Truncated {schema}.{table} Prior to load")
        print("")
        print(f"Loading {schema}.{table} ")



        cols =", ".join([f"${i}" for i in range(1,utility_file_length+1)])

        query = f"""COPY INTO {schema}.{table}
                    FROM (
                    SELECT {cols}
                    FROM @{external_stage}/{utility_file}/{load_date}/)
                    ENFORCE_LENGTH = True
                        FILE_FORMAT = (
                        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
                        type = csv 
                        FIELD_DELIMITER = ','
                        COMPRESSION = AUTO
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        EMPTY_FIELD_AS_NULL = True
                        NULL_IF = ('NULL','null','','None')
                        VALIDATE_UTF8 = False
                        )
                        ;

                    """

        results=cs.execute(query)
        rows=cs.fetchall()
        df=pd.DataFrame(rows, columns= [desc[0] for desc in cs.description])

        print(f"Succesfully loaded {utility_file} data into {schema}.{table} with {len(df)} files ")
        
        bi_query = f"""create or replace table bi.bills as select *  from {schema}.{table}; """
        cs.execute(bi_query)
        bi_validation = "select count(*) from bi.bills"
        results=cs.execute(bi_validation)
        rows=cs.fetchall()
        bi_df=pd.DataFrame(rows, columns= [desc[0] for desc in cs.description])
        
        print(f"Succesfully loaded BI.BILLS ")
           
    elif utility_file=='intervals':
        utility_file_length=13
        table='INTERVALS_RAW_SRC'
        
        print("")
        print(f"loading {utility_file} data for files ingested on {load_date}")
        print("")
        con=snowflake_connection(schema)
        cs=con.cursor()
        print(f"Truncating {schema}.{table} Prior to Load")
        cs.execute(f"""truncate {schema}.{table};""")
        print(f"Succesfully Truncated {schema}.{table} Prior to load")
        print("")
        print(f"Loading {schema}.{table} ")



        cols =", ".join([f"${i}" for i in range(1,utility_file_length+1)])

        query = f"""COPY INTO {schema}.{table}
                    FROM (
                    SELECT {cols}
                    FROM @{external_stage}/{utility_file}/{load_date}/)
                    ENFORCE_LENGTH = True
                        FILE_FORMAT = (
                        ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
                        type = csv 
                        FIELD_DELIMITER = ','
                        COMPRESSION = AUTO
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        EMPTY_FIELD_AS_NULL = True
                        NULL_IF = ('NULL','null','','None')
                        VALIDATE_UTF8 = False
                        )
                        ;

                    """

        results=cs.execute(query)
        rows=cs.fetchall()
        df=pd.DataFrame(rows, columns= [desc[0] for desc in cs.description])

        print(f"Succesfully loaded {utility_file} data into {schema}.{table} with {len(df)} files ")
        
        bi_query = f"""create or replace view bi.intervals as (
                        select 
                          to_timestamp(INTERVAL_START, 'MM/DD/YYYY HH24:MI') as start_ts,
                          to_timestamp(INTERVAL_end,'MM/DD/YYYY HH24:MI') as end_ts,
                          right(UTILITY_SERVICE_ADDRESS,6) as zip_code,
                          *
                        from STAGE.intervals_raw_src 
                          where utility <> 'DEMO'
                          order by meter_uid, start_ts asc
                         );
                          """
        
        cs.execute(bi_query)
        bi_validation = "select count(*) from bi.intervals"
        results=cs.execute(bi_validation)
        rows=cs.fetchall()
        bi_df=pd.DataFrame(rows, columns= [desc[0] for desc in cs.description])
        print(f"Succesfully loaded BI.INTERVALS ")
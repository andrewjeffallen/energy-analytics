from snowflake import connector
import boto3
from aws import get_secret, get_aws_session

def snowflake_connection(secret_name):
  
  snowflake_credentials = get_secret(f"{secret_name}")

  con = snowflake.connector.connect(
                user=sf_user,
                password=snowflake_credentials.get("password"),
                account=snowflake_credentials.get("account"),
                warehouse=snowflake_credentials.get("wh"),
                database=snowflake_credentials.get("sfdb"),
                schema=snowflake_credentials.get("schema")
                )
  return con


with con.cursor() as cur:
    cur.execute(f"""COPY INTO STAGE.{table_name} 
                FROM (
                SELECT *
                FROM @EXT_STAGE_NM/ # add s3 prefix)
                ENFORCE_LENGTH = True
                    FILE_FORMAT = (
                    type = csv 
                    FIELD_DELIMITER = ','
                    COMPRESSION = AUTO
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    EMPTY_FIELD_AS_NULL = True
                    NULL_IF = ('NULL','null','')
                    VALIDATE_UTF8 = False
                    )
                
Pattern = '.*.csv'; """)

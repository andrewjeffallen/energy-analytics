con = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA
                )


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

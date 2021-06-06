def snowflake_connection(schema):
    
    creds = get_secret('utility-database-credentials')
    
    con = snowflake.connector.connect(
        user=creds.get("user"),
        password=creds.get("password"),
        account=cred.get("acount"),
        warehouse=cred.get("warehouse"),
        database=cred.get("database"),
        schema=schema,
        role=cred.get('role')
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

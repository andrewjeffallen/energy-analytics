def snowflake_connection(schema):
    con = snowflake.connector.connect('credentials')
    return con

def load_data_to_snowflake(schema,table,external_stage,utility_file):
    load_date = date.today().strftime("%Y-%m-%d")
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
    
    if utility_file=='bills':
        utility_file_length=16
    elif utility_file=='intervals':
        utility_file_length=13
    
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

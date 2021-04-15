

with con.cursor() as cur:
  cur.execute(f"""
COPY INTO { table } FROM @EXTERNAL_STAGE_LOCATION_NAME
    STORAGE_INTEGRATION = myint
    FILE_FORMAT=(field_delimiter=',')
    PATTERN_STRING= '*.csv.gz'
""")

def execute_s3_copy_into_snowflake(
    stage_object, s3_client, **context,
):

        # running copy into
      if obj_metadata["details"]["truncate"]:
          query = f"""TRUNCATE {obj_metadata["target"]["schema"]}.{obj_metadata["target"]["table"]}"""
          run_query(query, "write", **context)

      columns = obj_metadata["columns"]
      stage_col = ", ".join([f"${col['source']}" for col in columns])
      table_col = ", ".join([col["target"] for col in columns])

      # Query Builder
      query = f"""
          COPY INTO {obj_metadata["target"]["schema"]}.{obj_metadata["target"]["table"]} (file_name, load_date, {table_col})
              FROM (
              SELECT  metadata$filename, 
                      {stage_col}
              FROM @{obj_metadata["source"]["external_stage"]}/{search_prefix})
              ENFORCE_LENGTH = {enforce_length}"""

      if file_type.upper() == "CSV":
          headers = obj_metadata["details"]["headers"]
          file_type_part = f"""
                  FILE_FORMAT = (
                  type = {file_type} 
                  FIELD_DELIMITER = '{obj_metadata['details']['delimiter']}'
                  COMPRESSION = AUTO
                  SKIP_HEADER = {validate_headers(headers)}
                  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                  EMPTY_FIELD_AS_NULL = True
                  NULL_IF = ('NULL','null','')
                  VALIDATE_UTF8 = False
                  )
              """
      elif file_type.upper() == "JSON":
          file_type_part = f"""
                  FILE_FORMAT = (
                  type = {file_type.upper()}
                  STRIP_OUTER_ARRAY=true
                  )
              """
      pattern_part = f"""Pattern = '{obj_metadata["source"]["pattern_string"]}';"""

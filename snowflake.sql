create or replace table stage.intervals_raw
(

  start_ts varchar(100),
  end_ts varchar(100),
  kwh number(38,3),
  meter_uid integer,
  authorization_uid integer,
  utility varchar(40),
  service_address varchar(10000)
);

COPY INTO STAGE.intervals_raw
                FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7
                FROM @utility_api_stage/intervals/)
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
                    );
                
                
create or replace view bi.intervals as (
select 
  to_timestamp(START_TS) as start_ts,
  to_timestamp(end_ts) as end_ts,
  kwh,
  meter_uid,
  authorization_uid ,
  utility ,
  service_address,
  right(service_address,6) as zip_code
from STAGE.intervals_raw 
 );


-- Intervals

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

create or replace table stage.intervals_raw_src
(
  meter_uid integer,
  utility varchar(10000),
  utility_service_id varchar(10000),
  utility_service_address varchar(10000),
  utility_meter_number varchar(10000),
  utility_tariff_name varchar(10000),
  interval_start varchar(10000),
  interval_end varchar(10000),
  interval_kWh number(38,4),
  net_kWh number(38,4),
  source varchar(10000),
  updated varchar(10000),
  interval_timezone varchar(10000)
);



COPY INTO STAGE.intervals_raw_src
                FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                FROM @utility_api_stage/intervals/historical)
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
  to_timestamp(INTERVAL_START, 'MM/DD/YYYY HH24:MI') as start_ts,
  to_timestamp(INTERVAL_end,'MM/DD/YYYY HH24:MI') as end_ts,
  right(UTILITY_SERVICE_ADDRESS,6) as zip_code,
  *
from STAGE.intervals_raw_src 
  where utility <> 'DEMO'
  order by meter_uid, start_ts asc
 );
 
 
 
 

create or replace table stage.bills_raw_src
(
meter_uid integer,
utility varchar(1000),
utility_service_id varchar(1000),
utility_billing_account varchar(1000),
utility_service_address varchar(1000),
utility_meter_number varchar(1000),
utility_tariff_name varchar(1000),
bill_start_date varchar(1000),
bill_end_date varchar(1000),
bill_days varchar(1000),
bill_statement_date varchar(1000),
bill_total_kWh number(38,2),
bill_total number(38,2),
source varchar(1000),
updated varchar(1000),
bill_volume varchar(1000),
bill_total_unit varchar(1000),
sce_details_voltage varchar(1000),
Winter_Usage_Mid_Peak_cost varchar(1000),
Winter_Usage_Mid_Peak_kwh varchar(1000),
Winter_Usage_Super_off_Peak_cost varchar(1000),
Winter_Usage_Super_off_Peak_kwh varchar(1000),
Winter_Usage_Off_Peak_cost varchar(1000),
Winter_Usage_Off_Peak_kwh varchar(1000),
Winter_Demand_Mid_Peak_cost varchar(1000),
Winter_Demand_Mid_Peak_kw varchar(1000),
Demand_cost number(38,2),
Demand_kw number(38,3),
power_reactive_usage_kvarh varchar(1000),
Summer_Usage_Off_Peak_cost varchar(1000),
Summer_Usage_Off_Peak_kwh varchar(1000),
Summer_Usage_Mid_Peak_cost varchar(1000),
Summer_Usage_Mid_Peak_kwh varchar(1000),
Summer_Usage_On_Peak_cost varchar(1000),
Summer_Usage_On_Peak_kwh varchar(1000),
Summer_Demand_On_Peak_cost varchar(1000),
Summer_Demand_On_Peak_kw varchar(1000)
);


COPY INTO STAGE.bills_raw_src
                FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37
                FROM @utility_api_stage/bills/)
                ENFORCE_LENGTH = True
                    FILE_FORMAT = (
                    type = csv 
                    FIELD_DELIMITER = ','
                    COMPRESSION = AUTO
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    EMPTY_FIELD_AS_NULL = True
                    NULL_IF = ('NULL','null','','None')
                    VALIDATE_UTF8 = False
                    )
                    on_error=SKIP_FILE
                    ;

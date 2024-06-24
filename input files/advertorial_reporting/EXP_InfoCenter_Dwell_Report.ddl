-- Old advertorial dwell time report. Decommissioned as of 7/7/2021. Replaced with RUM reporting in ${pipeline_schema}InfoCenter_Dwell_Time_By_Unique

CREATE SCHEMA IF NOT EXISTS ${pipeline_schema};

CREATE TABLE IF NOT EXISTS ${pipeline_schema}.Infocenter_dwell_report
(

    locale   STRING,
    display_name   STRING,
    marketing_campaign_id   INT,
    device_class   STRING,
    ip_country   STRING,
    dwell_time   INT,
    dwell_samples_total  INT,
    dwell_samples_10_to_15min   INT

)
PARTITIONED BY (ds string)
STORED AS ORC;
CREATE SCHEMA IF NOT EXISTS ${pipeline_schema};

CREATE TABLE IF NOT EXISTS ${pipeline_schema}.Infocenter_GAadsol
(

    locale  STRING,
    device_class  STRING,
    display_name STRING,
    page_action STRING,
    action_count  INT,
    unique_users  INT

)
PARTITIONED BY (ds string)
STORED AS ORC;
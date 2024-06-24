CREATE SCHEMA IF NOT EXISTS ${pipeline_schema};

CREATE TABLE IF NOT EXISTS ${pipeline_schema}.Infocenter_pageviews_uniques
(

    locale  STRING,
    device_class  STRING,
    display_name STRING,
    pageviews  INT,
    unique_users  INT

)
PARTITIONED BY (ds string)
STORED AS ORC;
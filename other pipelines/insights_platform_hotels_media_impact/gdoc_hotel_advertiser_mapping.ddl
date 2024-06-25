CREATE TABLE IF NOT EXISTS ${hive_pipeline_schema}.gdoc_hotel_advertiser_mapping (
    timestamp           VARCHAR,
    hotel_brand_name    VARCHAR,
    hotel_brand_code    VARCHAR,
    dfp_advertiser_id   VARCHAR,
    sales_region        VARCHAR
    )
WITH (FORMAT = 'ORC')
;
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.article_sponsored_profile_key (
    article_id STRING,
    campaign_id INT,
    order_id INT,
    url STRING,
    locale STRING,
    campaign_start_date STRING,
    campaign_end_date STRING,
    sponsor_id INT,
    sponsor_name STRING,
    dates STRING
);

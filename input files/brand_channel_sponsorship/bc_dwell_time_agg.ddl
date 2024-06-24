CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_dwell_time_agg (
  uniques                 STRING,
  locale                  STRING,
  platform                STRING,
  page                    STRING,
  bc_geo_id               INT,
  bc_geo_name             STRING,
  total_dwell_time        INT,
  ds                      DATE,
  marketing_campaign_id   INT,
  user_country_id         INT,
  user_country_name       STRING,
  op1_order_id            STRING
);

GRANT SELECT ON &{pipeline_schema_sf}.bc_dwell_time_agg TO PUBLIC;

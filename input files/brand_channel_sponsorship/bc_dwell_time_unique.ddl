
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_dwell_time_unique
( unique_id               STRING,
  locale                  STRING,
  platform                STRING,
  page                    STRING,
  bc_geo_id               INT,
  bc_geo_name             STRING,
  dwell_time              INT,
  ds date
);

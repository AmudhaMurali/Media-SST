
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_unique (

ds                      date,
unique_id               string,
marketing_campaign_id   int,
user_country_id         int,
user_country_name       string,
op1_order_id            string,
bc_geo_id               int,
bc_geo_name             string,
os_type                 string,
locale                  string,
element_type            string,
had_interaction         boolean,
impressions             int,
interactions            int,
video_completion_rate   DOUBLE


);

GRANT SELECT ON &{pipeline_schema_sf}.bc_unique TO PUBLIC;

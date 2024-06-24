
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_view_rate (

ds                      date,
bc_geo_id               int,
bc_geo_name             string,
url                     string,
marketing_campaign_id   int,
user_country_id         int,
user_country_name       string,
locale                  string,
client_type             string,
saw_brand_channel_page  boolean,
unique_users            int,
retention_users         int

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_view_rate TO PUBLIC;

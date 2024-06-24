
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_media_impact (

ds                      date,
saw_brand_channel_page  boolean,
bc_geo_id               string,
bc_geo_name             string,
client_type             string,
user_country_name       string,
locale                  string,
marketing_campaign_id   string,
uniques                 int,
total_pageviews_by_users                int,
total_attraction_pageviews_by_users     int,
total_accomodation_pageviews_by_users   int,
total_poi_pageviews_by_users            int,
total_clicks_by_users                   double,
total_estimated_bookings_by_users       double

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_media_impact TO PUBLIC;


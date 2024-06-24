
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_agg (

ds                      date,
element_type            string,
bc_geo_id               int,
bc_geo_name             string,
op1_order_id            string,
user_country_id         int,
user_country_name       string,
marketing_campaign_id   int,
os_type                 string,
locale                  string,
video_completion_rate   double,
uniques                 int,
bc_home_page_views      int,
impressions             int,
interactions            int,
uniques_w_interactions  int,
attraction_pageviews    int,
accomodation_pageviews  int,
restaurant_pageviews    int,
total_pageviews         int,
poi_pageviews           int,
estimated_bookings      double,
bc_entry_point          string

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_agg TO PUBLIC;


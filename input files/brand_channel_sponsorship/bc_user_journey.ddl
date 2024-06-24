
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_user_journey (

ds                      date,
saw_brand_channel_page  boolean,
bc_entry_point          string,
bc_geo_id               string,
bc_geo_name             string,
client_type             string,
user_country_name       string,
locale                  string,
marketing_campaign_id   string,
uniques                 int,
bc_interactions                         int,
bc_shelf_impressions                    int,
bc_page_views                           int,
moved_to_view_poi_uniques               int,
moved_to_view_hotel_poi_uniques         int,
moved_to_view_attraction_poi_uniques    int,
moved_to_view_eatery_poi_uniques        int,
moved_back_to_bc_page_uniques           int,
moved_to_view_trips_uniques             int,
moved_to_view_articles_uniques          int,
entered_bc_via_listing_page_uniques     int,
entered_bc_via_search_uniques           int,
entered_bc_outside_of_trip_uniques      int,
total_pageviews_by_users                int,
total_attraction_pageviews_by_users     int,
total_accomodation_pageviews_by_users   int,
total_poi_pageviews_by_users            int,
total_estimated_bookings_by_users       double

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_user_journey TO PUBLIC;


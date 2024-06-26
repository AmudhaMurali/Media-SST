
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ip_competitive (

data_type               string,
ds	                    date,
brand_type	            string,
brand	                string,
brand_code	            string,
parent_brand	        string,
comp_for_country	    string,
comp_brand_code         string,

star_rating             double,
num_properties          int,

uniques                 int,
pageviews               int,
clicks                  int,
est_bookings            double,
bookings                int,
avg_nights_per_booking  double,
avg_nightly_spend       double,
avg_num_guests          double,
avg_num_rooms           double,
avg_days_out            double,
avg_properties_viewed_pp double,
num_property_metrics    int,
uniques_pp              double,
pageviews_pp            double,
clicks_pp               double,
est_bookings_pp         double,
bookings_pp             double,

management_responses	int,
review_count	        int,
published_photo_count	int,
published_video_count	int,
avg_bubble_score	    double,
rate_cleanliness	    double,
rate_location	        double,
rate_room	            double,
rate_service	        double,
rate_sleep	            double,
rate_value	            double,
num_property_ratings    int,
management_responses_pp double,
review_count_pp         double,
published_photo_count_pp double,
published_video_count_pp double,

os_group                string,
user_country_id         int,
user_country_name       string,
user_region_id          int,
user_region_name        string,
uniques_market          int

);

GRANT SELECT ON &{pipeline_schema}.ip_competitive TO PUBLIC;
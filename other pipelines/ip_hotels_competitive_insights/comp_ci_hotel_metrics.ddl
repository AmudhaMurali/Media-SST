
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.comp_ci_hotel_metrics (

ds	                    date,
brand_type	            string,
brand	                string,
brand_code	            string,
parent_brand	        string,
comp_for_country	    string,
--user_country_name       string,
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
bookings_pp             double

);

GRANT SELECT ON &{pipeline_schema}.comp_ci_hotel_metrics TO PUBLIC;
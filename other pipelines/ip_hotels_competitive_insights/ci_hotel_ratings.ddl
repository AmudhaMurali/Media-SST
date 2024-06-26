
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_hotel_ratings (

ds                  date,
brand_code          string,
brand_name          string,
parent_brand_name   string,
--sales_region        string,
property_country_id int,
property_country_name   string,
star_rating         double,
management_responses    int,
review_count        int,
published_photo_count   int,
published_video_count   int,
avg_bubble_score    double,
rate_cleanliness    double,
rate_location       double,
rate_room           double,
rate_service        double,
rate_sleep          double,
rate_value          double,
num_property_ratings int,
management_responses_pp double,
review_count_pp     double,
published_photo_count_pp double,
published_video_count_pp double

);

GRANT SELECT ON &{pipeline_schema}.ci_hotel_ratings TO PUBLIC;





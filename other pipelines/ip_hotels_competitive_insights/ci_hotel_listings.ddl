
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_hotel_listings (

ds                      date,
brand_code              string,
brand_name              string,
parent_brand_name       string,
--sales_region            string,
property_country_id     int,
property_country_name   string,
star_rating             double,
num_properties          int

);

GRANT SELECT ON &{pipeline_schema}.ci_hotel_listings TO PUBLIC;





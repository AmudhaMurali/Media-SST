
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_hotel_market (

ds                      date,
brand_code              string,
brand_name              string,
parent_brand_name       string,
property_country_id        int,
property_country_name      string,
--sales_region            string,
os_group                string,
user_country_id         int,
user_country_name       string,
user_region_id          int,
user_region_name        string,
uniques                 int

);

GRANT SELECT ON &{pipeline_schema}.ci_hotel_market TO PUBLIC;
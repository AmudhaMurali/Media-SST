
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.comp_ci_hotel_market (

ds	                    date,
brand_type	            string,
brand	                string,
brand_code	            string,
parent_brand	        string,
comp_for_country	    string,
os_group                string,
user_country_id         int,
user_country_name       string,
user_region_id          int,
user_region_name        string,
uniques                 int

);

GRANT SELECT ON &{pipeline_schema}.comp_ci_hotel_market TO PUBLIC;
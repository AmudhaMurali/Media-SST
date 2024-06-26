
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.comp_ci_hotel_listings (

ds	                    date,
brand_type	            string,
brand	                string,
brand_code	            string,
parent_brand	        string,
comp_for_country	    string,
star_rating	            double,
num_properties	        int

);

GRANT SELECT ON &{pipeline_schema}.comp_ci_hotel_listings TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_top_cities_geo (
            geo_id              INT,
            geo_name            STRING,
            user_country_name   STRING,
            user_market         STRING,
            top_city_id         INT,
            top_city_name       STRING,
            uniques             INT,
            pvs                 INT,
            bookings            INT,
            clicks              INT,
            city_rank           INT

);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_top_cities_geo TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_top_cities_geo TO PUBLIC;
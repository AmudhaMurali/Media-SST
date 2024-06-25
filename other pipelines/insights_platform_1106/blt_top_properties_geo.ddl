CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_top_properties_geo (
        geo_id              INT,
        geo_name            STRING,
        user_country_name   STRING,
        user_market         STRING,
        place_type          STRING,
        location_id         INT,
        property_name       STRING,
        loc_city_name       STRING,
        uniques             INT,
        pvs                 INT,
        bookings            INT,
        clicks              INT,
        location_rank       INT

);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_top_properties_geo TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_top_properties_geo TO PUBLIC;
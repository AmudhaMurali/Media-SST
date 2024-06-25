CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_top_locations_geo (
        ds                  DATE,
        os_group            STRING,
        user_location_id    INT,
        user_country_id     INT,
        user_country_name   STRING,
        location_id         INT,
        property_name       STRING,
        loc_city_id         INT,
        loc_city_name       STRING,
        loc_region_id       INT,
        loc_region_name     STRING,
        geo_id              INT,
        geo_name            STRING,
        place_type          STRING,
        uniques             INT,
        pvs                 INT,
        bookings            INT,
        clicks              INT

);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_top_locations_geo TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_top_locations_geo TO PUBLIC;
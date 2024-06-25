CREATE TABLE IF NOT EXISTS &{pipeline_schema}.dmo_top_location_pv (
        ds               DATE,
        user_country_name_agg   STRING,
        user_market         STRING,
        top_city_id         INT,
        top_city_name       STRING,
        uniques             INT,
        pvs                 INT,
        bookings            INT,
        clicks              INT,
        city_rank_pv        INT,
        total_pvs           INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.dmo_top_location_pv TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.dmo_top_location_pv TO PUBLIC;
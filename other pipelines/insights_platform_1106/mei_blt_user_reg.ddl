CREATE TABLE IF NOT EXISTS &{pipeline_schema}.baseline_trends_uregion (
        ds                          date,
        os_group                    string,
        geo_id                      int,
        geo_name                    string,
        user_country_id             int,
        user_country                string,
        user_market                 string,
        u_region_id                 int,
        user_region                 string,
        user_reg_per_country_rank   int,
        place_type_grouping         string,
        uniques                     int,
        pvs                         int,
        clicks                      int,
        bookings                    int
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.baseline_trends_uregion TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.baseline_trends_uregion TO PUBLIC;
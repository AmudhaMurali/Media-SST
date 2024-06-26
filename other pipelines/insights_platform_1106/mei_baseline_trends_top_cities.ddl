CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_top_ucity (
    geo_id                          int,
    geo_name                        string,
    user_country_rank               int,
    user_country                    string,
    user_city_per_country_rank      int,
    user_city                       string,
    uniques                         int,
    geo_uniques                     int,
    country_uniques                 int,
    city_uniques                    int
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_top_ucity TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_top_ucity TO PUBLIC;
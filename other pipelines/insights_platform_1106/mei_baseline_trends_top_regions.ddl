CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_top_ureg (
    geo_id                          int,
    geo_name                        string,
    user_country_rank               int,
    user_country                    string,
    user_reg_per_country_rank       int,
    user_region                     string,
    uniques                         int,
    geo_uniques                     int,
    country_uniques                 int,
    region_uniques                  int
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_top_ureg TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_top_ureg TO PUBLIC;
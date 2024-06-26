CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_top_city_markets (
            advertiser_id           INT,
            advertiser_name         STRING,
            ad_name_formatted       STRING,
            ad_geo_id               INT,
            geo_id                  INT,
            geo_name                STRING,
            ad_rank                 INT,
            ad_user_city            STRING,
            ad_reg_of_city          STRING,
            ad_uniques              INT,
            comp_rank               INT,
            comp_user_city          STRING,
            comp_reg_of_city        STRING,
            comp_uniques            INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_top_city_markets TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_top_city_markets TO PUBLIC;
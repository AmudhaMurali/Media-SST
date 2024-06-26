CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_top_markets (
           advertiser_id        INT,
           advertiser_name      STRING,
           ad_name_formatted    STRING,
           ad_geo_id            INT,
           ad_geo_name          STRING,
           ad_rank              INT,
           market_type          STRING,
           market               STRING,
           market_in            STRING,
           ad_uniques           INT,
           comp_rank            INT,
           comp_market          STRING,
           comp_market_in       STRING,
           comp_uniques         INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_top_markets TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_top_markets TO PUBLIC;



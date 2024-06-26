CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_daily_comp_ratings (
            ds                      DATE,
            advertiser_id           INT,
            advertiser_name         STRING,
            ad_name_formatted       STRING,
            ad_geo_id               INT,
            ad_geo_name             STRING,
            poi_type                STRING,
            tvlr_type               STRING,
            ag_traveler_rating      DOUBLE,
            ag_num_reviews          INT,
            cs_avg_traveler_rating  DOUBLE,
            cs_avg_num_reviews      INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_daily_comp_ratings TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_daily_comp_ratings TO PUBLIC;
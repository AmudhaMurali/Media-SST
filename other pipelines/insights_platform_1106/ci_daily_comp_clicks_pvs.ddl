CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_daily_comp_clicks_pvs (
         ds                         DATE,
         advertiser_id              INT,
         advertiser_name            STRING,
         ad_name_formatted          STRING,
         ad_geo_id                  INT,
         ad_geo_name                STRING,
         ag_uniques                 INT,
         ag_uu_pv                   INT,
         ag_pvs                     INT,
         ag_uu_cc                   INT,
         ag_commerce_clicks         INT,
         ag_bookings                INT,
         cs_uniques                 INT,
         cs_uu_pv                   INT,
         cs_pvs                     INT,
         cs_uu_cc                   INT,
         cs_commerce_clicks         INT,
         cs_bookings                INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_daily_comp_clicks_pvs TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_daily_comp_clicks_pvs TO PUBLIC;

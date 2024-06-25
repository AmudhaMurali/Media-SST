CREATE TABLE IF NOT EXISTS &{pipeline_schema}.dfp_pio_ad_name (
    advertiser_id       INT,
  	advertiser_name     STRING,
  	ad_name_formatted   STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.dfp_pio_ad_name TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.dfp_pio_ad_name TO PUBLIC;

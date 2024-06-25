CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_uu_ad_share (
                	ds                  DATE,
  	                geo_id              INT,
  	                geo_name            STRING,
  	                view_type           STRING,
  	                uniques             INT,
  	                advertiser_id       INT,
                    advertiser_name     STRING,
                    ad_name_formatted   STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_uu_ad_share TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_uu_ad_share TO PUBLIC;
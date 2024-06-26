CREATE TABLE IF NOT EXISTS &{pipeline_schema}.advertiser_oxford_loc_mapping (
             ox_loc_id   	    INT,
             ox_name            STRING,
             advertiser_id      INT,
             advertiser_name    STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.advertiser_oxford_loc_mapping TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.advertiser_oxford_loc_mapping TO PUBLIC;

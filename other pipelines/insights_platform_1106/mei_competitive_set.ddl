CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_competitive_set (
             advertiser_id      INT,
             advertiser_name    STRING,
             ad_name_formatted  STRING,
             ad_geo_id   	    INT,
             ad_geo_name        STRING,
             similar_geo_id     INT,
             similar_geo_name   STRING,
             sim_geo_state      STRING,
             similarity         FLOAT,
             rank               INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_competitive_set TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_competitive_set TO PUBLIC;
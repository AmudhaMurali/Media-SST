CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_competitive_geo_set (
             ad_geo   	    INT,
             ad_geo_name    STRING,
             similar_geo    INT,
             sim_geo_name   STRING,
             sim_geo_state  STRING,
             similarity     FLOAT,
             rank           INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_competitive_geo_set TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_competitive_geo_set TO PUBLIC;

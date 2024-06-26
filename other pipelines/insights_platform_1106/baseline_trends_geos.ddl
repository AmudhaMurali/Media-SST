CREATE TABLE IF NOT EXISTS &{pipeline_schema}.baseline_trends_geos (
             geo_id   	INT,
             geo_name   STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.baseline_trends_geos TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.baseline_trends_geos TO PUBLIC;

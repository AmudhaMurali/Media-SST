CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_geo_list (
             geo_id   	INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.blt_geo_list TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.blt_geo_list TO PUBLIC;

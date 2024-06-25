CREATE TABLE IF NOT EXISTS &{pipeline_schema}.geo_to_location (
             location_id   	            INT,
             location_name              STRING,
             location_placetype_name    STRING,
             geo_id                     INT,
             geo_name                   STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.geo_to_location TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.geo_to_location TO PUBLIC;
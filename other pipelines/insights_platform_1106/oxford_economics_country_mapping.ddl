CREATE TABLE IF NOT EXISTS &{pipeline_schema}.oxford_economics_country_mapping (
             primaryname   	STRING,
             id             INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.oxford_economics_country_mapping TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.oxford_economics_country_mapping TO PUBLIC;

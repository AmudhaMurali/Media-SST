CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_all_competitve_geos (
             advertiser_id  INT,
             ad_geo_id      INT,
             similar_geo_id INT,
             similarity     FLOAT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_all_competitve_geos TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_all_competitve_geos TO PUBLIC;
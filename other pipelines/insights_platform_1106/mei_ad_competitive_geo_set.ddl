CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_ad_competitive_geo_set (
            ADVERTISER_ID       INT,
            ADVERTISER_NAME     STRING,
            AD_NAME_FORMATTED   STRING,
            AD_GEO_ID           INT,
            AD_GEO_NAME         STRING,
            SIMILAR_GEO_ID      INT,
            SIMILAR_GEO_NAME    STRING,
            SIMILARITY          FLOAT,
            RANK                INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_ad_competitive_geo_set TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_ad_competitive_geo_set TO PUBLIC;
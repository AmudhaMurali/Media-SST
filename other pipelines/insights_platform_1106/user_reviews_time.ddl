CREATE TABLE IF NOT EXISTS &{pipeline_schema}.user_reviews_time (
            ds                  DATE,
            advertiser_id       INT,
            advertiser_name     STRING,
            ad_name_formatted   STRING,
            poi_type            STRING,
            location_id         INT,
            loc_geo_id          INT,
            loc_country_id      INT,
            loc_country_name    STRING,
            user_geo_id         INT,
            user_country_id     INT,
            user_country_name   STRING,
            tvlr_type           STRING,
            user_rating         INT,
            lang                STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.user_reviews_time TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.user_reviews_time TO PUBLIC;



CREATE TABLE IF NOT EXISTS &{pipeline_schema}.user_ratings_geo (
            ds                  DATE,
            geo_id              INT,
            geo_name            STRING,
            loc_country_id      INT,
            loc_country_name    STRING,
            poi_type            STRING,
            user_country_id     INT,
            user_country_name   STRING,
            user_market         STRING,
            tvlr_type           STRING,
            traveler_rating     DOUBLE,
            review_type         STRING,
            segment_type        STRING,
            num_reviews         INT

);

--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.user_ratings_geo TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.user_ratings_geo TO PUBLIC;

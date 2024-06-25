CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ad_hotel_star_ratings_90d (
                advertiser_id           INT,
                advertiser_name         STRING,
                ad_name_formatted       STRING,
                geo_id                  INT,
                geo_name                STRING,
                acc_bookings            INT,
                w_avg_star_rating       FLOAT,
                median_star_rating      FLOAT,
                mode_star_rating        FLOAT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ad_hotel_star_ratings_90d TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ad_hotel_star_ratings_90d TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.booking_distance_adv (
            ds                      DATE,
            advertiser_id           INT,
            advertiser_name         STRING,
            ad_name_formatted       STRING,
            advertiser_category     STRING,
            user_geo_id             INT,
            user_country_id         INT,
            user_country_name       STRING,
            user_market             STRING,
            distance_traveler       STRING,
            short_distance_tvlr     BOOLEAN,
            long_distance_tvlr      BOOLEAN,
            intl_traveler           BOOLEAN,
            uniques                 INT,
            acc_bookers             INT,
            attr_bookers            INT,
            acc_bookings            INT,
            total_nights_booked     INT,
            acc_nights_per_booking  DOUBLE,
            acc_w_avg_rooms         DOUBLE,
            acc_w_avg_guests        DOUBLE,
            acc_w_avg_spend         DOUBLE,
            w_avg_booking_window    DOUBLE,
            attr_bookings           INT,
            attr_w_avg_spend        DOUBLE,
            attr_w_avg_guests       DOUBLE
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.booking_distance_adv TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.booking_distance_adv TO PUBLIC;
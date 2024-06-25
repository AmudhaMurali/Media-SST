CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_economic_impact_ta_only (
      ds                            DATE,
      os_type                       STRING,
      os_group                      STRING,
      uniques                       INT,
      saw_campaign                  BOOLEAN,
      imp_ds                        DATE,
      daydif                        INT,
      advertiser_id                 INT,
      advertiser_name               STRING,
      ad_name_formatted             STRING,
      advertiser_category           STRING,
      user_country_id               INT,
      user_country_name             STRING,
      user_market                   STRING,
      ad_country_id                 INT,
      traveler_type                 STRING,
      booked_acc                    BOOLEAN,
      acc_bookings                  INT,
      num_acc_booked                INT,
      total_nights_booked           INT,
      avg_nightly_booking_rate      DOUBLE,
      avg_num_booking_guests        DOUBLE,
      avg_num_booking_rooms         DOUBLE,
      avg_nights_per_booking        DOUBLE,
      acc_clicks                    INT,
      booked_attr                   BOOLEAN,
      attr_bookings                 INT,
      avg_attr_spend                DOUBLE,
      avg_attr_guests               DOUBLE
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_economic_impact_ta_only TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_economic_impact_ta_only TO PUBLIC;



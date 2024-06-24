
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_dwell_time_wp (
            ds                      DATE,
            trip_id                 INT,
            trip_title              STRING,
            username                STRING,
            display_name            STRING,
            os_type                 STRING,
            locale                  STRING,
            uniques                 INT,
            total_dwell_time        INT,
            samples_under_10_min    INT,
            samples_over_10_min     INT,
            total_samples           INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_dwell_time_wp TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.sponsored_trips_dwell_time_wp TO PUBLIC;

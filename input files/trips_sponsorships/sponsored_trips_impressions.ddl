
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_impressions (
            ds                      DATE,
            trip_id                 INT,
            trip_title              STRING,
            username                STRING,
            display_name            STRING,
            os_type                 STRING,
            locale                  STRING,
            uniques                 INT,
            impressions             INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_impressions TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.sponsored_trips_impressions TO PUBLIC;
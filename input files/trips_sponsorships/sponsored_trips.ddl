CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips (
    TRIP_ID     INT,
    USERNAME    STRING
);


--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips TO PUBLIC;
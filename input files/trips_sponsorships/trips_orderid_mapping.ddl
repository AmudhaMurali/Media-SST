

CREATE TABLE IF NOT EXISTS &{pipeline_schema}.trips_orderid_mapping (

        ds                  date,
        trip_id             int,
        order_id            int


);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.trips_orderid_mapping TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.trips_orderid_mapping TO PUBLIC;

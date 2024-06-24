CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_social (
    DS           DATE,
    OS_TYPE      STRING,
    LOCALE       STRING,
    TRIP_ID      INT,
    TITLE        STRING,
    TRIP_LIKES   INT,
    TRIP_REPOSTS INT,
    TRIP_SHARES  INT,
    USERNAME     STRING,
    op_advertiser_id    int,
    advertiser_name     string,
    sales_order_id      int,
    sales_order_name    string,
    industry            string,
    region              string,
    account_exec        string,
    user_country_name   string
);


--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_social TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips_social TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.baseline_trends_base (
    DS                  DATE,
    OS_TYPE             STRING,
    OS_GROUP            STRING,
    GEO_ID              INT,
    GEO_NAME            STRING,
    GEO_DEPTH           STRING,
    PLACE_TYPE_GROUPING STRING,
    USER_CONTINENT      STRING,
    USER_COUNTRY_ID     INT,
    USER_COUNTRY        STRING,
    USER_REGION_ID      INT,
    USER_REGION         STRING,
    USER_CITY_ID        INT,
    USER_CITY           STRING,
    PVS                 INT,
    CLICKS              INT,
    BOOKINGS            INT,
    UNIQUES_COUNT       INT,
    DMO_TYPE            STRING
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.baseline_trends_base TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.baseline_trends_base TO PUBLIC;

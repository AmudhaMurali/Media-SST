CREATE TABLE IF NOT EXISTS &{pipeline_schema}.trip_sponsors (
    USERNAME         STRING,
    SAFE_USERNAME    STRING,
    DISPLAY_NAME     STRING,
    USER_ID          INT,
    CREATION_DATE    DATE
);


--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.trip_sponsors TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.trip_sponsors TO PUBLIC;
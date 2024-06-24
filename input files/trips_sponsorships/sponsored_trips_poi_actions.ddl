CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_poi_actions (
       ds               DATE,
       unique_id        STRING,
       os_type          STRING,
       locale           STRING,
       user_id          INT,
       username         STRING,
       trip_title       STRING,
       trip_desc        STRING,
       tripid           INT,
       saveType         STRING,
       detailid         INT,
       detailid_name    STRING,
       clicks           INT,
       likes            INT,
       see_more         INT,
       shares           INT,
       maps             INT,
       USER_COUNTRY_NAME STRING,
       marketing_campaign_id INT
);

--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_poi_actions TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips_poi_actions TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_poi_saves_wp (
       ds               DATE,
       unique_id        STRING,
       os_type          STRING,
       locale           STRING,
       user_id          INT,
       username         STRING,
       tripid           INT,
       trip_title       STRING,
       detailid         INT,
       detailid_name    STRING,
       saves            INT,
       USER_COUNTRY_NAME  STRING,
       marketing_campaign_id INT
);

--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_poi_saves_wp TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips_poi_saves_wp TO PUBLIC;

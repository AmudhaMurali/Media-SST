CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_poi (
       ds               DATE,
       os_type          STRING,
       locale           STRING,
       tripid           INT,
       trip_title       STRING,
       detailid         INT,
       detailid_name    STRING,
       user_id          INT,
       username         STRING,
       clicks           INT,
       likes            INT,
       see_more         INT,
       shares           INT,
       maps             INT,
       saves            INT,
       op_advertiser_id    int,
       advertiser_name     string,
       sales_order_id      int,
       sales_order_name    string,
       industry            string,
       region              string,
       account_exec        string,
       USER_COUNTRY_NAME   string,
       marketing_campaign_id    int,
       uniques_with_poi_actions int,
       uniques_with_poi_saves   int
);

--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_poi TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips_poi TO PUBLIC;
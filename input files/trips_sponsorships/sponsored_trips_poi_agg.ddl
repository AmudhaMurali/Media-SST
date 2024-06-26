CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_poi_agg (
       ds               DATE,
       os_type          STRING,
       locale           STRING,
       tripid           INT,
       trip_title       STRING,
       detailid         INT,
       detailid_name    STRING,
       user_id          INT,
       username         STRING,
       op_advertiser_id    int,
       advertiser_name     string,
       sales_order_id      int,
       sales_order_name    string,
       industry            string,
       region              string,
       account_exec        string,
       USER_COUNTRY_NAME   string,
       marketing_campaign_id    int,
       poi_map_click    INT,
       poi_carousel     INT,
       poi_readmore     INT,
       poi_clickthr     INT,
       poi_map_clickthr INT,
       poi_save         INT,
       uniques_with_poi_actions int
);
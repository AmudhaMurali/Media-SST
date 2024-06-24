
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_pvs (

        ds                  date,
        trip_id             int,
        trip_title          STRING,
        username            STRING,
        display_name        STRING,
        locale              string,
        user_country_id     int,
        user_country_name   string,
        os_type             string,
        marketing_campaign_id  int,
        op_advertiser_id    int,
        advertiser_name     string,
        order_id            int,
        sales_order_name    string,
        industry            string,
        region              string,
        account_exec        string,

        uniques             int,
        pageviews           int

       -- utm_source          string,
       -- utm_medium          string


);
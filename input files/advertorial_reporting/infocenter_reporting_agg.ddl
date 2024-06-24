CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.infocenter_reporting_agg (
            ds                  date,
            servlet_name        string,
            display_name        string,
            op_advertiser_id    int,
            advertiser_name     string,
            order_id            int,
            sales_order_name    string,
            industry            string,
            advertiser_region   string,
            account_exec        string,
            utm_source          string,
            client_type         string,
            user_country_name   string,
            locale              string,
            uniques             int,
            pageviews           int,
            traffic_source      string
);

GRANT SELECT ON &{pipeline_schema_sf}.infocenter_reporting_agg TO PUBLIC;
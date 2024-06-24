CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.sponsored_articles_dwell_time_agg (
        ds                  date,
        url_id              string,
        op_advertiser_id    int,
        advertiser_name     string,
        order_id            int,
        sales_order_name    string,
        industry            string,
        advertiser_region   string,
        account_exec        string,
        article_title       string,
        sponsor_status      string,
        sponsor_name        string,
        use_sponsor_info    string,
        utm_source          string,
        locale              string,
        user_country_name   string,
        platform            string,
        uniques             int,
        total_dwell_time_ms int,
        avg_dwell_time_ms   double
);

GRANT SELECT ON &{pipeline_schema_sf}.sponsored_articles_dwell_time_agg TO PUBLIC;
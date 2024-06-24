CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.article_reporting (

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
        MARKETING_CAMPAIGN_ID  int,
        campaign_start_date date,
        campaign_end_date   date,
        item_id             string,
        item_name           string,
        item_type           string,
        action_type         string,
        action_sub_type     string,
        total_impression    int,
        total_users         int,
        total_interaction   int

);

GRANT SELECT ON &{pipeline_schema_sf}.article_reporting TO PUBLIC;
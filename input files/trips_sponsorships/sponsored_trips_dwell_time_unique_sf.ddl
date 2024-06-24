CREATE TABLE IF NOT EXISTS &{pipeline_schema}.sponsored_trips_dwell_time_unique_sf(

    ds              date,
    page_name       string,
    servlet_name    string,
    trip_id         int,
    trip_title      string,
    username        string,
    display_name    string,
    locale          string,
    url             string,
    url_rum         string,
    user_country_name   string,
    os_type_name    string,
    unique_id       string,
    dwell_time      int,
    op_advertiser_id    int,
    advertiser_name     string,
    sales_order_id      int,
    sales_order_name    string,
    industry            string,
    region              string,
    account_exec        string,
    marketing_campaign_id int
    --utm_source          string,
    --utm_medium          string

);

--GRANT OWNERSHIP ON TABLE &{pipeline_schema}.sponsored_trips_dwell_time_unique_sf TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
--GRANT SELECT ON &{pipeline_schema}.sponsored_trips_dwell_time_unique_sf TO PUBLIC;
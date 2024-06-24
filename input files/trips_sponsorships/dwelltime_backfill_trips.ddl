
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.dwelltime_backfill_trips
(

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
    marketing_campaign_id int,
    ds date

);
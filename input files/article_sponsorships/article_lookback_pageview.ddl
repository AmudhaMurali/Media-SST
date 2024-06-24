CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.article_lookback_pageview (
        ds                  date,
        LOCALE              string,
        USER_COUNTRY_ID     int,
        USER_COUNTRY_NAME   string,
    COMMERCE_COUNTRY_ID     int,
    MARKETING_CAMPAIGN_ID   int,
        utm_source          string,
        OS_PLATFORM         string,
        item_id             string,
        item_name           string,
        item_type           string,
        order_id            int,
        url_id              string,
        unique_id           string
);

GRANT SELECT ON &{pipeline_schema_sf}.article_lookback_pageview TO PUBLIC;
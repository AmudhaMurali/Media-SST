CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.sponsored_articles_dwell_time_unique_mcid

(

    page_name           string,
    url_article_id      string,
    article_title       string,
    sponsor_name        string,
    use_sponsor_info    string,
    locale              string,
    url                 string,
    url_rum             string,
    user_country_name   string,
    os_type_name        string,
    unique_id           string,
    marketing_campaign_id int,
    dwell_time          int,
    ds                  date

);

GRANT SELECT ON &{pipeline_schema_sf}.sponsored_articles_dwell_time_unique_mcid TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.sponsored_articles (

    id                  string,
    space               string,
    url_article_id      string,
    servlet_name        string,
    locale              string,
    article_title       string,
    sponsor_name        string,
    use_sponsor_info    string,
    campaign_start_date date,
    campaign_end_date   date,
    sponsor_url         string,
    created_at          date,
    last_updated_at     date,
    is_original         boolean

);



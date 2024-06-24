CREATE TABLE IF NOT EXISTS &{pipeline_schema}.cta_click_count (

    DS                          Date,
    locale                      STRING,
    user_agent                  STRING,
    servlet                     STRING,
    geo_id                      INT,
    campaign_id                 INT,
    title_cta_click             INT,
    link_cta_click              INT

);
GRANT SELECT ON &{pipeline_schema}.cta_click_count TO PUBLIC;
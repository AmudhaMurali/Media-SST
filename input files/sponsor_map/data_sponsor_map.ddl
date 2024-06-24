CREATE TABLE IF NOT EXISTS &{pipeline_schema}.data_sponsor_map (

    DS                          Date,
    locale                      STRING,
    user_agent                  STRING,
    servlet                     STRING,
    geo_id                      INT,
    geo_name                    STRING,
    campaign_id                 INT,
    campaign_name               STRING,
    map_open_ct                 INT,
    pin_hover_tab_ct            INT,
    title_cta_click             INT,
    link_cta_click              INT

);
GRANT SELECT ON &{pipeline_schema}.data_sponsor_map TO PUBLIC;
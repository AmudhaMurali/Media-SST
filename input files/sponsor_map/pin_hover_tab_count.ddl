CREATE TABLE IF NOT EXISTS &{pipeline_schema}.pin_hover_tab_count (

    DS                          Date,
    locale                      STRING,
    user_agent                  STRING,
    servlet                     STRING,
    geo_id                      INT,
    campaign_id                 INT,
    pin_hover_tab_ct            INT

);
GRANT SELECT ON &{pipeline_schema}.pin_hover_tab_count TO PUBLIC;
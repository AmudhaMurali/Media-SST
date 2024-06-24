CREATE TABLE IF NOT EXISTS &{pipeline_schema}.map_open_count (

    DS                          Date,
    locale                      STRING,
    user_agent                  STRING,
    servlet                     STRING,
    geo_id                      INT,
    campaign_id                 INT,
    map_open_ct                 INT

);
GRANT SELECT ON &{pipeline_schema}.map_open_count TO PUBLIC;

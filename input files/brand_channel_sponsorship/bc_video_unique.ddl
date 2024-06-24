
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_video_unique (

        ds                              date,
        unique_id                       string,
        marketing_campaign_id           string,
        user_country_id                 int,
        user_country_name               string,
        bc_geo_id                       int,
        bc_geo_name                     string,
        os_type                         string,
        locale                          string,
        bc_shelf                        string,
        video_completion_rate           DOUBLE,
        impressions                     int,
        interactions                    int

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_video_unique TO PUBLIC;


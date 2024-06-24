CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_video_impressions (
        ds                      date,
        unique_id               string,
        os_type                 string,
        locale                  string,
        marketing_campaign_id   string,
        user_country_id         int,
        user_country_name       string,
        bc_geo_id               int,
        bc_geo_name             string,
        impression_id           string,
        page_uid                string,
        placement               string,
        bc_shelf                string,
        event_timestamp_ms      TIMESTAMP

);

CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_video_inter (

        ds                      date,
        unique_id               string,
        interaction_id          string,
        page_uid                string,
        item_id                 string,
        item_type               string,
        geo_id                  string,
        video_completion_rate   DOUBLE,
        number_of_video_views   INT,
        event_timestamp_ms      TIMESTAMP



);

GRANT SELECT ON &{pipeline_schema_sf}.bc_video_inter TO PUBLIC;

--
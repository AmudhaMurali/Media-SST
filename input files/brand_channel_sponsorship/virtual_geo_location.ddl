
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.virtual_geo_location (
        last_updated                date,
        virtual_geo_id              int,
        virtual_geo_name            string,
        virtual_geo_placetypeid     int,
        virtual_geo_placetype_name  string,
        child_id                    int,
        child_id_name               string,
        child_id_placetypeid        int,
        child_id_placetypename      string,
        location_id                 int,
        location_id_name            string,
        location_id_placetypeid     int,
        location_id_placetypename   string
);

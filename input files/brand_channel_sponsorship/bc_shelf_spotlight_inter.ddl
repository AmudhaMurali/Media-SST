CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_shelf_spotlight_inter (

        ds                  date,
        unique_id           string,
        feed_section_id     string,
        puid                string,
        context             string,
        element             string,
        ui_element_type     string,
        ui_element_source   string

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_shelf_spotlight_inter TO PUBLIC;

--
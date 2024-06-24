CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_shelf_spotlight_imps (
        ds                      date,
        unique_id               string,
        os_type                 string,
        locale                  string,
        marketing_campaign_id   string,
        user_country_id         int,
        user_country_name       string,
        bc_geo_id               int,
        bc_geo_name             string,
        feed_section_id         string,
        puid                    string,
        cluster_id              string,
        placement               string,
        bc_shelf                string,
        item_category           string,
        curated_shelf_type      string,
        shelf_title_key         string,
        list_type               string,
        position                int,
        item_list_id            array

);

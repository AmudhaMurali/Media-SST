
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.bc_shelf_spotlight_update (

        ds                              date,
        unique_id                       string,
        os_type                         string,
        locale                          string,
        marketing_campaign_id           string,
        user_country_id                 int,
        user_country_name               string,
        bc_geo_id                       int,
        bc_geo_name                     string,
        bc_shelf                        string,
        impressions                     int,
        interactions                    int

);

GRANT SELECT ON &{pipeline_schema_sf}.bc_shelf_spotlight_update TO PUBLIC;
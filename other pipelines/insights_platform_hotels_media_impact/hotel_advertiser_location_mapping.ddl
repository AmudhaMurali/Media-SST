CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.hotel_advertiser_location_mapping (

    dfp_advertiser_id     int,
    advertiser_name       string,
    ad_name_formatted     string,
    location_id           int,
    brand_name            string,
    parent_brand_name     string,
    sales_region          string,
    hotel_country_name    string,
    hotel_state_name      string

);


grant select on &{pipeline_schema_sf}.hotel_advertiser_location_mapping  to public;

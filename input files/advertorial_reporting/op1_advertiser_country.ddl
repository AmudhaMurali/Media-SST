CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.op1_advertiser_country (
        op_advertiser_id    int,
        advertiser_name     string,
        country_name        string,
        country             string,
        region              string,
        industry            string
);

GRANT SELECT ON &{pipeline_schema_sf}.op1_advertiser_country TO PUBLIC;
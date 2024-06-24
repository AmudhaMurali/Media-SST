CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.article_order_unique_id_mapping (
        order_id            int,
        url_id              string
);

GRANT SELECT ON &{pipeline_schema_sf}.article_order_unique_id_mapping TO PUBLIC;

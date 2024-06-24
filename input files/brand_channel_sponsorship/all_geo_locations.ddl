CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.all_geo_locations (
             last_updated               DATE,
             location_id   	            INT,
             location_name              STRING,
             location_placetype_name    STRING,
             geo_id                     INT,
             geo_name                   STRING
);

GRANT SELECT ON &{pipeline_schema_sf}.all_geo_locations TO PUBLIC;

--- for each ad geo and competitive geo in the ML output (mei_competitive_geo_set)
--- this table returns all the location ids corresponding to each geo

--- This table will not be used for reporting ---


begin;
delete from &{pipeline_schema}.geo_to_location;

INSERT INTO  &{pipeline_schema}.geo_to_location
SELECT DISTINCT lh.location_id                  as location_id,
                lh.location_name                as location_name,
                lh.location_placetype_name      as location_placetype_name,
                lh.hierarchical_location_id     as geo_id,
                lh.hierarchical_location_name   as geo_name
FROM display_ads.sales.location_hierarchy lh
WHERE hierarchical_location_id <> 1
AND hierarchical_depth <> 0
;

commit;
----------------------------------------------------------------------------------
-- combines locatons for all virtual geos and regular geos
----------------------------------------------------------------------------------

begin;
delete from &{pipeline_schema_sf}.all_geo_locations;



insert into &{pipeline_schema_sf}.all_geo_locations


select  '&{start_dt}' as last_updated,
       location_id,
       location_id_name as location_name,
       location_id_placetypename as location_placetype_name,
       virtual_geo_id as geo_id,
       virtual_geo_name as geo_name
from display_ads.sales.virtual_geo_location

UNION ALL

select '&{start_dt}' as last_updated,
       location_id,
       location_name,
       location_placetype_name,
       geo_id,
       geo_name
from display_ads.sales.geo_to_location


;

commit;

-- Creating a mapping table that will update daily (so will not be distinct) with all sponsored articles and associated Operative One order ids
-- To be used for dwell time tables (because RUM doesnt have custom data tracking)


BEGIN;
DELETE FROM &{pipeline_schema_sf}.article_order_id_mapping
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.article_order_id_mapping
select distinct imp.ds,
                custom_data,
                cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int) as order_id,
                cast(split_part(regexp_substr(imp.custom_data,'Articles-l\\w*-'),'-',2) as string) as url_id
from user_tracking.public.user_impressions imp
where imp.ds between '&{start_dt}' and '&{end_dt}'
and imp.item_type in ('articleSponsorInfoImpression','articleLinkClick')
and lower(imp.custom_data) like '%operativeorderid%'
;

commit;
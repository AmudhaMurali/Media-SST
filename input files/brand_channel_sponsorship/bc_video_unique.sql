begin;

delete from &{pipeline_schema_sf}.bc_video_unique
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_video_unique

select imp.ds,
       imp.unique_id,
       imp.marketing_campaign_id,
       imp.user_country_id,
       imp.user_country_name,
       imp.bc_geo_id,
       imp.bc_geo_name,
       imp.os_type,
       imp.locale,
       imp.bc_shelf,
       max(intr.video_completion_rate) as video_completion_rate,
       count(distinct imp.impression_id) as impressions,
       count(distinct intr.interaction_id) as interactions
from &{pipeline_schema_sf}.bc_video_impressions imp
LEFT JOIN &{pipeline_schema_sf}.bc_video_inter intr on imp.ds = intr.ds and imp.unique_id = intr.unique_id and imp.page_uid = intr.page_uid
where imp.ds between '&{start_dt}' and '&{end_dt}'
group by imp.ds,
         imp.unique_id,
         imp.marketing_campaign_id,
         imp.user_country_id,
         imp.user_country_name,
         imp.bc_geo_id,
         imp.bc_geo_name,
         imp.os_type,
         imp.locale,
         imp.bc_shelf

;

commit;

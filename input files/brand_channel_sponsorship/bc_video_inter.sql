begin;

delete from &{pipeline_schema_sf}.bc_video_inter
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_video_inter

select vc.ds,
       vc.unique_id,
       vc.interaction_id,
       vc.page_uid,
       vc.item_id,
       vc.item_type,
       vc.geo_id,
       vv.video_completion_rate,
       vv.number_of_video_views,
       vc.event_timestamp_ms
from user_tracking.public.user_interactions vc

LEFT JOIN (select ds,
       imp.page_uid
       ,MAX(cast(left(replace(custom_data_json['viewedVideoProgress'],'"', ''),len(custom_data_json['viewedVideoProgress'])-1) as double)/100) as video_completion_rate
       ,count(*) as number_of_video_views
from user_tracking.public.user_interactions imp
where imp.ds between '&{start_dt}' and '&{end_dt}' and item_type in ('BrandChannelVideoView')
group by ds, page_uid) vv on vv.page_uid = vc.page_uid and vv.ds = vc.ds
where vc.ds between '&{start_dt}' and '&{end_dt}' and item_type in ('BrandChannelVideoClick')

;


commit;


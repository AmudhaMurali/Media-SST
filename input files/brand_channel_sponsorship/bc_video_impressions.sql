-- daily video impressions on brand channel pages - pulled from user_impressions IIS tracking

begin;

delete from &{pipeline_schema_sf}.bc_video_impressions
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_video_impressions

Select
imp.ds,
    imp.unique_id,
    au.os_type,
    imp.locale,
    au.marketing_campaign_id,
    au.commerce_country_id as user_country_id,
    au.user_country_name,
    imp.geo_id as bc_geo_id,
    bc.sponsor_name as bc_geo_name,
    impression_id,
    imp.page_uid,
    imp.page as placement,
    item_type as bc_shelf,
    imp.event_timestamp_ms
from user_tracking.public.user_impressions imp
join rio_sf.rust.a_unique_users au on imp.unique_id = au.unique_id and imp.ds = au.ds
                                   and au.ds between '&{start_dt}' and '&{end_dt}'
left join (select distinct location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bc on bc.location_id = imp.geo_id -- only brand channels, change to JOIN for PROD
where imp.ds between '&{start_dt}' and '&{end_dt}'
and imp.item_type in ('BrandChannelVideoImpression')


;

commit;

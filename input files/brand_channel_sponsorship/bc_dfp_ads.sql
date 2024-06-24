----------------------------------------------------------------------------------
---- pulls dfp ads from all brand channel /tourism pages
----------------------------------------------------------------------------------

begin;

delete from &{pipeline_schema_sf}.bc_dfp_ads
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_dfp_ads


select dfp.ds,
       dfp.unique_id,
       'Display Ad' as element_type,
        case when ses.os_type = 7 then 'windows'
             when ses.os_type = 8 then 'osx'
             when ses.os_type = 11 then 'linux'
             when ses.os_type = 4 then 'iphone_hybrid_app'
             when ses.os_type = 5 then 'iphone_native_app'
             when ses.os_type = 9 then 'iphone_browser'
             when ses.os_type = 2 then 'android_hybrid_app'
             when ses.os_type = 3 then 'android_native_app'
             when ses.os_type = 12 then 'android_browser'
             when ses.os_type = 14 then 'ipad_hybrid_app'
             when ses.os_type = 10 then 'ipad_browser'
             when ses.os_type = 6 then 'android_tablet_hybrid_app'
             when ses.os_type = 13 then 'android_tablet_browser'
             when ses.os_type = 18 then 'ipad_native_app'
             when ses.os_type = 19 then 'windows_tablet_hybrid_app'
             when ses.os_type = 20 then 'apple_tv_native_app'
             when ses.os_type = 1 then 'other'
             when ses.os_type = 15 then 'bot'
             when ses.os_type = 16 then 'other_tablet'
             when ses.os_type = 17 then 'other_phone' else null end as device,
       ses.pos as locale,
       dfp.geo_id as bc_geo_id,
       bc.sponsor_name as bc_geo_name,
       null as user_country_id,
       ses.ip_country as user_country_name,
       null as marketing_campaign_id, -- doesnt contextually make sense here
       count(*) as impressions,
       to_boolean(case when dfp.is_clicked = 1 then 1 else 0 end) as had_interaction,
       sum(case when dfp.is_clicked = 1 then 1 else 0 end) as interactions
from display_ads.sales.vw_f_dfp_hourly_master_impressions dfp
join rio_sf.public.rio_sessions ses on lower(dfp.session_id)  = lower(ses.session_id) and dfp.ds = ses.ds
join (select distinct location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bc on bc.location_id = dfp.geo_id -- only brand channels
where dfp.ds between '&{start_dt}' and '&{end_dt}'
    and ses.os_type <> 15 -- not bot
    and dfp.session_id IS NOT NULL
    and dfp.unique_id is not null
    and ses.is_blessed = 1
    and (dfp.pos in ('horizon') or servlet_name in ('tourism', 'mobiletourism')) and lower(dfp.pv_id) in (select distinct lower(page_uid) from DISPLAY_ADS.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily where bc_page_name = 'brand_channel_home' and ds between '&{start_dt}' and '&{end_dt}')
group by dfp.ds, dfp.unique_id, 'Display Ad', case when ses.os_type = 7 then 'windows' when ses.os_type = 8 then 'osx'
         when ses.os_type = 11 then 'linux' when ses.os_type = 4 then 'iphone_hybrid_app' when ses.os_type = 5 then 'iphone_native_app'
         when ses.os_type = 9 then 'iphone_browser' when ses.os_type = 2 then 'android_hybrid_app' when ses.os_type = 3 then 'android_native_app'
         when ses.os_type = 12 then 'android_browser'when ses.os_type = 14 then 'ipad_hybrid_app' when ses.os_type = 10 then 'ipad_browser'
         when ses.os_type = 6 then 'android_tablet_hybrid_app' when ses.os_type = 13 then 'android_tablet_browser' when ses.os_type = 18 then 'ipad_native_app'
         when ses.os_type = 19 then 'windows_tablet_hybrid_app' when ses.os_type = 20 then 'apple_tv_native_app' when ses.os_type = 1 then 'other'
         when ses.os_type = 15 then 'bot' when ses.os_type = 16 then 'other_tablet' when ses.os_type = 17 then 'other_phone' else null end, ses.pos,
         dfp.geo_id, bc.sponsor_name, null, ses.ip_country, null, to_boolean(case when dfp.is_clicked = 1 then 1 else 0 end)

;

commit;

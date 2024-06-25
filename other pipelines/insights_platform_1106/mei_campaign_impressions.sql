------------------------------------------------------------------
-- counts impressions, clicks, uniques and CTR for each campaign
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.mei_campaign_impressions
where ds between '&{start_dt}' and '&{end_dt}';

insert into  &{pipeline_schema}.mei_campaign_impressions
select imp.ds                               as ds,
       imp.advertiser_id                    as advertiser_id,
       adname.advertiser_name               as advertiser_name,
       adname.ad_name_formatted             as ad_name_formatted,
       adcat.advertiser_labels              as advertiser_category,
       imp.order_id                         as order_id,
       imp.native_ad_format_name            as native_ad_format_name,
       reg.country_name                     as user_country,
       (case when uu.os_type in ('android_browser', 'iphone_browser', 'other_phone') then 'Mobile Web'
        when uu.os_type in ('android_native_app', 'ipad_native_app','iphone_native_app') then 'Native App'
        when uu.os_type in ('android_tablet_browser', 'ipad_browser', 'other_tablet') then 'Tablet Web'
        when uu.os_type in ('linux', 'osx', 'other', 'windows') then 'Desktop' else 'Other' end)
                                            as os_group,
       count(distinct imp.unique_id)        as imp_uniques_count,
       sum(imp.viewable_impression_counts)  as imp_viewable_ad_impressions,
       sum(imp.viewable_click_counts)       as imp_viewable_ad_clicks,
       sum(imp.total_impression_counts)     as imp_total_ad_impressions,
       sum(imp.total_click_counts)          as imp_total_ad_clicks
from display_ads.sales.user_order_impressions_daily_new imp
join (select distinct dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.advertiser_id = imp.advertiser_id
left join rio_sf.display_sales.f_dfp_advertisers adcat on imp.ADVERTISER_ID = adcat.id -- to get advertiser category
left join &{pipeline_schema}.dfp_ad_name adname on imp.advertiser_id = adname.advertiser_id -- ad name
join  USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS uu ON imp.unique_id = uu.unique_id
                                   AND imp.ds = uu.ds
                                   AND uu.ds between '&{start_dt}' and '&{end_dt}'
left join tripdna.revops.dna_geo_hierarchy reg on uu.USER_IP_LOCATION_ID = reg.GEO_ID -- user's geo
where imp.ds between '&{start_dt}' and '&{end_dt}'
group by imp.ds, imp.advertiser_id, adname.advertiser_name, adname.ad_name_formatted, adcat.advertiser_labels,
         imp.order_id, imp.native_ad_format_name, reg.country_name, os_group
;

commit;
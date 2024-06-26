------------------------------------------------------------------
-- uses total impressions (viewable and non-viewable) as base table
-- and joins to interactions to see if those who have been exposed
-- to an advertiser's ad have interacted with an advertiser's geo
-- within 90 days of ad exposure
------------------------------------------------------------------


begin;

delete from &{snow_database}.&{pipeline_schema}.mei_campaign_imps_lookback
where ds = '&{start_dt}';

insert into  &{pipeline_schema}.mei_campaign_imps_lookback
with interactions as (
    select UNIQUE_ID,
           ADVERTISER_ID,
           min(ds) as first_visit
    from display_ads.sales.user_advertiser_interactions_daily
    where ds between '&{start_dt}' and '&{start_dt_p90}'---- interaction between imp date and imp date + 90
    group by UNIQUE_ID, ADVERTISER_ID
),
  impressions as (
  select ds                             as ds,
         unique_id                      as unique_id,
         advertiser_id                  as advertiser_id,
         sum(TOTAL_IMPRESSION_COUNTS)   as total_ad_impressions,
         sum(TOTAL_CLICK_COUNTS)        as total_ad_clicks
    from display_ads.sales.user_order_impressions_daily
    where ds = '&{start_dt}'
    group by ds, unique_id, ADVERTISER_ID
)
select imp.ds                                                       as ds
     , imp.UNIQUE_ID is not null                                    as saw_campaign
     , ifnull(sum(imp.total_ad_impressions), 0)                     as imp_total_ad_impressions
     , ifnull(sum(imp.total_ad_clicks), 0)                          as imp_total_ad_clicks
     , sum(case when imp.UNIQUE_ID is not null then 1 else 0 end)   as num_saw_campaign
     , count(DISTINCT imp.unique_id)                                AS imp_uniques_count
     , inter.first_visit                                            as ds_inter
     , datediff(day, imp.ds, inter.first_visit)                     as daydif
     , inter.UNIQUE_ID is not null                                  as saw_and_looked
     , sum(case when inter.UNIQUE_ID is not null then 1 else 0 end) as num_saw_and_looked
     , imp.ADVERTISER_ID                                            as advertiser_id
     , adname.ADVERTISER_NAME                                       as advertiser_name
     , adname.AD_NAME_FORMATTED                                     as ad_name_formatted
     , adcat.ADVERTISER_LABELS                                      as advertiser_category
     , reg.COUNTRY_NAME                                             as COUNTRY_NAME
     , reg.REGION1_NAME                                             as REGION
     , uu.os_type                                                   as OS_TYPE
     , (case when uu.os_type in ('android_browser',
                                'iphone_browser',
                                'other_phone') then 'Mobile Web'
            when uu.os_type in ('android_native_app',
                                'ipad_native_app'
                                ,'iphone_native_app') then 'Native App'
            when uu.os_type in ('android_tablet_browser',
                                'ipad_browser',
                                'other_tablet') then 'Tablet Web'
            when uu.os_type in ('linux',
                                'osx',
                                'other',
                                'windows') then 'Desktop'
            else 'Other' end)                                       as OS_GROUP
     , uu.LOCALE                                                    as LOCALE
from impressions imp
left join interactions inter on imp.UNIQUE_ID = inter.UNIQUE_ID
                             and imp.ADVERTISER_ID = inter.ADVERTISER_ID
                             and (inter.first_visit between '&{start_dt}' and '&{start_dt_p90}')
JOIN (select distinct dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.ADVERTISER_ID = imp.advertiser_id
LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on imp.ADVERTISER_ID = adcat.id -- to get advertiser category
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on imp.advertiser_id = adname.advertiser_id -- ad name
JOIN  USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS uu ON imp.unique_id = uu.unique_id AND imp.ds = uu.ds
LEFT JOIN tripdna.revops.dna_geo_hierarchy reg on uu.USER_IP_LOCATION_ID = reg.GEO_ID -- user's geo
where imp.ds = '&{start_dt}'
group by imp.ds
     , saw_campaign
     , inter.first_visit
     , saw_and_looked
     , imp.ADVERTISER_ID
     , adname.ADVERTISER_NAME
     , adname.AD_NAME_FORMATTED
     , adcat.ADVERTISER_LABELS
     , reg.COUNTRY_NAME
     , reg.REGION1_NAME
     , uu.os_type
     , os_group
     , uu.LOCALE
;

commit;
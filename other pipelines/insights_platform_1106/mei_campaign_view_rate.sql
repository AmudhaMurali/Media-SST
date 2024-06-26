------------------------------------------------------------------
-- uses total impressions (viewable and non-viewable) as base table
-- and joins to interactions to see if those who have been exposed
-- to an advertiser's ad have interacted with an advertiser's geo
-- within 90 days of ad exposure
-- THIS IS FOR VIEW RATE ONLY
------------------------------------------------------------------


begin;

delete from &{snow_database}.&{pipeline_schema}.mei_campaign_view_rate
where ds = '&{start_dt_m90}';

insert into  &{pipeline_schema}.mei_campaign_view_rate
with interactions as (
    select unique_id,
           advertiser_id,
           min(ds) as first_visit
    from display_ads.sales.user_advertiser_interactions_daily
    where ds between '&{start_dt_m90}' and '&{start_dt}'
    group by unique_id, advertiser_id
),
  impressions as (
  select ds                             as ds,
         unique_id                      as unique_id,
         advertiser_id                  as advertiser_id,
         sum(total_impression_counts)   as total_ad_impressions,
         sum(total_click_counts)        as total_ad_clicks
    from display_ads.sales.user_order_impressions_daily
    where ds = '&{start_dt_m90}'
    group by ds, unique_id, advertiser_id
)
select imp.ds                                                       as ds
     , ifnull(sum(imp.total_ad_impressions), 0)                     as imp_total_ad_impressions
     , ifnull(sum(imp.total_ad_clicks), 0)                          as imp_total_ad_clicks
     , count(distinct imp.unique_id)                                as imp_uniques_count
     , sum(case when imp.unique_id is not null then 1 else 0 end)   as num_saw_campaign
     , inter.first_visit                                            as ds_inter
     , datediff(day, imp.ds, inter.first_visit)                     as daydif
     , inter.unique_id is not null                                  as saw_and_looked
     , sum(case when inter.unique_id is not null then 1 else 0 end) as num_saw_and_looked
     , imp.advertiser_id                                            as advertiser_id
     , adname.advertiser_name                                       as advertiser_name
     , adname.ad_name_formatted                                     as ad_name_formatted
     , adcat.advertiser_labels                                      as advertiser_category
     , reg.country_name                                             as user_country
     , (case when uu.os_type in ('android_browser', 'iphone_browser', 'other_phone') then 'Mobile Web'
        when uu.os_type in ('android_native_app', 'ipad_native_app','iphone_native_app') then 'Native App'
        when uu.os_type in ('android_tablet_browser', 'ipad_browser', 'other_tablet') then 'Tablet Web'
        when uu.os_type in ('linux', 'osx', 'other', 'windows') then 'Desktop' else 'Other' end)
                                                                    as os_group
from impressions imp
left join interactions inter on imp.unique_id = inter.unique_id
                             and imp.advertiser_id = inter.advertiser_id
                             and (inter.first_visit between '&{start_dt_m90}' and '&{start_dt}')
JOIN (select distinct dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.advertiser_id = imp.advertiser_id
LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on imp.advertiser_id = adcat.id -- to get advertiser category
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on imp.advertiser_id = adname.advertiser_id -- ad name
JOIN USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS uu ON imp.unique_id = uu.unique_id AND imp.ds = uu.ds
LEFT JOIN tripdna.revops.dna_geo_hierarchy reg on uu.USER_IP_LOCATION_ID = reg.geo_id -- user's geo
where imp.ds = '&{start_dt_m90}'
group by imp.ds, inter.first_visit, datediff(day, imp.ds, inter.first_visit), inter.unique_id is not null, imp.advertiser_id,
         adname.advertiser_name, adname.ad_name_formatted, adcat.advertiser_labels, reg.country_name, os_group
;

commit;
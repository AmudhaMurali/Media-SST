


begin;

delete from &{pipeline_schema_sf}.hotel_mei_campaign_view_rate
where ds = '&{start_dt_m90}';

insert into  &{pipeline_schema_sf}.hotel_mei_campaign_view_rate
with interactions as (
    select unique_id,
           advertiser_id,
           min(ds) as first_visit
    from &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily --display_ads.sales.user_advertiser_interactions_daily
    where ds between '&{start_dt_m90}' and '&{start_dt}'
    group by unique_id, advertiser_id
),
  impressions as (
  select ds                             as ds,
         unique_id                      as unique_id,
         advertiser_id                  as advertiser_id,
         sum(TOTAL_IMPRESSION_COUNTS) as total_ad_impressions,
         sum(TOTAL_CLICK_COUNTS)      as total_ad_clicks
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
     , map.advertiser_name                                          as advertiser_name
     , map.ad_name_formatted                                        as ad_name_formatted
     , map.brand_name                                               as ad_brand_name
     , map.parent_brand_name                                        as ad_parent_brand_name
     , map.sales_region                                             as sales_region
     , adcat.advertiser_labels                                      as advertiser_category
     , reg.country                                                  as user_country
     , reg.continent_name                                           as user_continent
     , ctr.region                                                   as user_market
     , (case when uu.os_type in ('android_browser', 'iphone_browser', 'other_phone') then 'Mobile Web'
        when uu.os_type in ('android_native_app', 'ipad_native_app','iphone_native_app') then 'Native App'
        when uu.os_type in ('android_tablet_browser', 'ipad_browser', 'other_tablet') then 'Tablet Web'
        when uu.os_type in ('linux', 'osx', 'other', 'windows') then 'Desktop' else 'Other' end)
                                                                    as os_group
from impressions imp
--imp  inter
left join interactions inter on imp.unique_id = inter.unique_id
                             and imp.advertiser_id = inter.advertiser_id
                             and (inter.first_visit between '&{start_dt_m90}' and '&{start_dt}')
-- match brand
JOIN (select distinct dfp_advertiser_id as advertiser_id, advertiser_name, ad_name_formatted,brand_name, parent_brand_name,sales_region
      from &{pipeline_schema_sf}.hotel_advertiser_location_mapping) map on map.advertiser_id = imp.advertiser_id
LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on imp.advertiser_id = adcat.id -- to get advertiser category
-- user info
JOIN (select unique_id, ds, max(os_type) as os_type, max(locale) as locale, max(user_location_id) as user_location_id
          from rio_sf.rust.a_unique_users
          where ds  = '&{start_dt_m90}'
          group by 1,2
        ) uu on imp.unique_id = uu.unique_id AND imp.ds = uu.ds
left join (select distinct GEO_ID, country,continent_name, state_name from rio_sf.hotels_sst.a_location_details_latest) reg
     on uu.user_location_id = reg.geo_id -- user's geo
left join rio_sf.finance.country_to_region ctr on reg.country = ctr.country
where imp.ds = '&{start_dt_m90}'
group by imp.ds, inter.first_visit, datediff(day, imp.ds, inter.first_visit), inter.unique_id is not null, imp.advertiser_id,
         map.advertiser_name, map.ad_name_formatted,map.brand_name, map.parent_brand_name,map.sales_region,
        adcat.advertiser_labels, reg.country, reg.continent_name,ctr.region, os_group
;

commit;
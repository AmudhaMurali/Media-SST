------------------------------------------------------------------
-- counts impressions, clicks, uniques and CTR for each campaign
------------------------------------------------------------------

begin;

delete from &{pipeline_schema_sf}.user_hotel_advertiser_impressions_daily
where ds between '&{start_dt}' and '&{end_dt}';

insert into  &{pipeline_schema_sf}.user_hotel_advertiser_impressions_daily
select imp.ds                               as ds,
       imp.advertiser_id                    as advertiser_id,
       map.advertiser_name               as advertiser_name,
       map.ad_name_formatted             as ad_name_formatted,
       --map.brand_name,
       --map.parent_brand_name,
       --map.sales_region,
       adcat.advertiser_labels              as advertiser_category,
       imp.order_id                         as order_id,
       imp.native_ad_format_name            as native_ad_format_name,
       reg.country                          as user_country,
       reg.CONTINENT_NAME                   as user_continent,
       ctr.region                           as user_market,
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
join (select distinct dfp_advertiser_id as advertiser_id, advertiser_name, ad_name_formatted
                      --,brand_name, parent_brand_name,sales_region
      from &{pipeline_schema_sf}.hotel_advertiser_location_mapping) map on map.advertiser_id = imp.advertiser_id
left join rio_sf.display_sales.f_dfp_advertisers adcat on imp.ADVERTISER_ID = adcat.id -- to get advertiser category
JOIN (select unique_id, ds, max(os_type) as os_type, max(locale) as locale, max(user_location_id) as user_location_id
          from rio_sf.rust.a_unique_users
          where ds between '&{start_dt}' and '&{end_dt}'
          group by 1,2
        ) uu on imp.unique_id = uu.unique_id AND imp.ds = uu.ds
left join (select distinct GEO_ID, country ,continent_name, state_name from rio_sf.hotels_sst.a_location_details_latest) reg
 on uu.USER_LOCATION_ID = reg.GEO_ID -- user's geo
left join rio_sf.finance.country_to_region ctr on reg.country = ctr.country
where imp.ds between '&{start_dt}' and '&{end_dt}'
group by imp.ds, imp.advertiser_id,map.advertiser_name,map.ad_name_formatted,--map.brand_name, map.parent_brand_name,map.sales_region,
         adcat.advertiser_labels, imp.order_id, imp.native_ad_format_name, reg.country, reg.CONTINENT_NAME, ctr.region, os_group
;

commit;
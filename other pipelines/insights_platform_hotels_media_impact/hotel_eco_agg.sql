

begin;

delete from &{pipeline_schema_sf}.hotel_eco_agg
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.hotel_eco_agg
WITH impressions as (
        select max(ds)                      as last_imp,
               unique_id                    as unique_id,
               advertiser_id                as advertiser_id,
               sum(TOTAL_IMPRESSION_COUNTS) as imp_total_ad_impressions,
               sum(TOTAL_CLICK_COUNTS)      as imp_total_ad_clicks
        from  display_ads.sales.user_order_impressions_daily
        where ds between '&{start_dt_m90}' and '&{end_dt}'
        group by unique_id, advertiser_id
    )

SELECT inter.ds                                                         as ds

     --user info
     , uu.OS_TYPE                                                       as os_type
     ,(case when uu.os_type in ('android_browser','iphone_browser','other_phone') then 'Mobile Web'
            when uu.os_type in ('android_native_app','ipad_native_app','iphone_native_app') then 'Native App'
            when uu.os_type in ('android_tablet_browser','ipad_browser','other_tablet') then 'Tablet Web'
            when uu.os_type in ('linux','osx','other', 'windows') then 'Desktop'
            else 'Other' end)                                           as os_group
     , uu.LOCALE                                                        as locale
     , reg.country                                                      as user_country
     , reg.continent_name                                               as user_continent
     , reg.state_name                                                   as user_state
     , ctr.REGION                                                       as user_market

     -- ad info
     , inter.ADVERTISER_ID                                              as advertiser_id
     , map.advertiser_name                                              as advertiser_name
     , map.ad_name_formatted                                            as ad_name_formatted
     , map.brand_name                                                   as ad_brand_name
     , map.parent_brand_name                                            as ad_parent_brand_name
     , inter.hotel_country                                              as hotel_country
     , map.sales_region                                                 as sales_region
     , adcat.ADVERTISER_LABELS                                          as advertiser_category
     ,(case when inter.hotel_country  = reg.country     then 'Domestic' else 'Foreign' end)
                                                                        as traveler_type

     -- metrics
     , imp.UNIQUE_ID is not null                                        as saw_campaign
     , imp.last_imp                                                     as imp_ds
     , datediff(day, imp.last_imp, inter.ds)                            as daydif
     , to_boolean(case when inter.hotel_estimated_bookings > 0 then 1 else 0 end)
                                                                        as user_booked_a_location
     , count(distinct inter.UNIQUE_ID)                                  as uniques_count
     , sum(inter.hotel_estimated_bookings)                              as hotel_estimated_bookings
     , sum(inter.hotel_estimated_bookings_distinct)                     as hotel_estimated_bookings_distinct
     , sum(inter.total_nights_booked)                                   as total_nights_booked
     , round(avg(inter.avg_nightly_booking_spend),2)                    as avg_nightly_booking_spend
     , round(avg(inter.avg_num_booking_guests), 1)                      as avg_num_booking_guests
     , round(avg(inter.avg_num_booking_rooms),1)                        as avg_num_booking_rooms
     , round(avg(inter.avg_daily_rate),2)                               as avg_daily_rate
     , round(avg(inter.avg_accomodation_days_out),2)                    as avg_accomodation_days_out
     , sum(inter.click_count)                                           as click_count
     , 'country_base'                                                   as split_column

FROM &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily inter
    LEFT JOIN impressions imp                               on imp.unique_id = inter.unique_id
                                                            and imp.advertiser_id = inter.advertiser_id
                                                            and (imp.last_imp between '&{start_dt_m90}' and '&{end_dt}')

    JOIN (select distinct dfp_advertiser_id as advertiser_id, advertiser_name, ad_name_formatted,brand_name, parent_brand_name,sales_region
      from &{pipeline_schema_sf}.hotel_advertiser_location_mapping) map on map.advertiser_id = inter.advertiser_id
    LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on inter.ADVERTISER_ID = adcat.id
    JOIN (select unique_id, max(os_type) as os_type, max(locale) as locale, max(user_location_id) as user_location_id
          from rio_sf.rust.a_unique_users
          where ds between '&{start_dt_m30}' and '&{end_dt}'
          group by 1
        ) uu on uu.unique_id = inter.unique_id
    left join (select distinct GEO_ID, country ,continent_name, state_name from rio_sf.hotels_sst.a_location_details_latest) reg
          on uu.USER_LOCATION_ID = reg.GEO_ID -- user's geo
    left join rio_sf.finance.country_to_region ctr on reg.country = ctr.country
WHERE inter.ds  between '&{start_dt}' and '&{end_dt}'
group by inter.ds,
     uu.OS_TYPE,
     os_group,
     uu.LOCALE,
     reg.COUNTRY,
     reg.continent_name,
     reg.state_name,
     ctr.REGION,
     inter.ADVERTISER_ID,
     map.advertiser_name,
     map.ad_name_formatted,
     map.brand_name,
     map.parent_brand_name,
     inter.hotel_country,
     map.sales_region,
     adcat.ADVERTISER_LABELS,
     traveler_type,
     saw_campaign,
     imp.last_imp,
     daydif,
     user_booked_a_location,
     split_column

;

commit;

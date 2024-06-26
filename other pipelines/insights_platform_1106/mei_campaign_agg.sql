------------------------------------------------------------------
-- uses interactions as base table and joins to total impressions
-- (viewable and non-viewable) to see if those who have interacted
-- in an advertiser's geo have been exposed to an ad (i.e. have
-- had an impression) within 90 days of interacting with geo
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.mei_campaign_agg
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.mei_campaign_agg
WITH impressions as (
        select max(ds)                      as last_imp,
               unique_id                    as unique_id,
               advertiser_id                as advertiser_id,
               sum(TOTAL_IMPRESSION_COUNTS) as total_ad_impressions,
               sum(TOTAL_CLICK_COUNTS)      as total_ad_clicks
        from  display_ads.sales.user_order_impressions_daily
        where ds between '&{start_dt_m90}' and '&{end_dt}'
        group by unique_id, advertiser_id
    ),
     interactions as (
         select ds,
                UNIQUE_ID,
                ADVERTISER_ID,
                sum(CLICK_COUNT)                          as click_count,
                sum(num_locations_clicked)                as num_locations_clicked,
                sum(hotel_bookings)                       as hotel_bookings, -- formerly ib count
                sum(NUM_LOCATIONS_BOOKED)                 as num_locations_booked,
                sum(pv_count)                             as PV_COUNT,
                sum(num_locations_pv)                     as num_locations_pv,
                sum(ACCOMODATION_CLICK_COUNT)             as ACCOMODATION_CLICK_COUNT,
                sum(ACCOMODATION_PV_COUNT)                as ACCOMODATION_PV_COUNT,
                sum(NUM_DISTINCT_ACCOMODATIONS_CLICKED)   as NUM_DISTINCT_ACCOMODATIONS_CLICKED,
                sum(num_distinct_accomodations_viewed)    as num_distinct_accomodations_viewed,
                sum(ATTRACTION_PV_COUNT)                  as ATTRACTION_PV_COUNT,
                sum(NUM_DISTINCT_ATTRACTIONS_VIEWED)      as NUM_DISTINCT_ATTRACTIONS_VIEWED,
                sum(EATERY_CLICK_COUNT)                   as EATERY_CLICK_COUNT,
                sum(EATERY_PV_COUNT)                      as EATERY_PV_COUNT,
                sum(NUM_DISTINCT_EATERIES_CLICKED)        as NUM_DISTINCT_EATERIES_CLICKED,
                sum(NUM_DISTINCT_EATERIES_VIEWED)         as NUM_DISTINCT_EATERIES_VIEWED,
                sum(OTHER_CLICK_COUNT)                    as OTHER_CLICK_COUNT,
                sum(NUM_DISTINCT_OTHER_LOCATIONS_CLICKED) as NUM_DISTINCT_OTHER_LOCATIONS_CLICKED,
                sum(total_nights_booked)                  as total_acc_nights_booked,
                avg(avg_nightly_booking_spend)            as avg_acc_nightly_spend,
                avg(avg_num_booking_guests)               as avg_acc_boooking_guests,
                avg(avg_num_booking_rooms)                as avg_acc_booking_rooms,
                sum(num_attr_bookings)                    as total_attr_bookings,
                avg(avg_attr_gross_spend_usd)             as avg_attr_spend,
                avg(avg_attr_num_guests)                  as avg_attr_guests
        from display_ads.sales.user_advertiser_interactions_daily
        where ds  between '&{start_dt}' and '&{end_dt}'
        group by ds, unique_id, advertiser_id
     )
SELECT inter.ds                                                         as ds
     , uu.OS_TYPE                                                       as os_type
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
            else 'Other' end)                                           as os_group
     , uu.LOCALE                                                        as locale
     , reg.COUNTRY_NAME                                                 as country
     , reg.REGION1_NAME                                                 as region
     , inter.ADVERTISER_ID                                              as advertiser_id
     , adname.ADVERTISER_NAME                                           as advertiser_name
     , adname.AD_NAME_FORMATTED                                         as ad_name_formatted
     , adcat.ADVERTISER_LABELS                                          as advertiser_category
     , inter.UNIQUE_ID is not null                                      as is_looker
     , sum(case when inter.UNIQUE_ID is not null then 1 else 0 end)     as num_lookers
     , count(distinct inter.UNIQUE_ID)                                  as uniques_count
     , imp.UNIQUE_ID is not null                                        as saw_campaign
     , imp.last_imp                                                     as imp_ds -- ******************************************* new addition
     , datediff(day, imp.last_imp, inter.ds)                            as daydif -- ******************************************* new addition
     , sum(case when imp.UNIQUE_ID is not null then 1 else 0 end)       as num_saw_campaign
     , count(case when (inter.num_locations_clicked > 0
                          or inter.num_locations_booked > 0
                          or inter.total_attr_bookings > 0
                          or inter.num_locations_pv > 0)
                          then inter.UNIQUE_ID else null end)           as uu_w_interactions -- user_clicked_a_location (aka user interacted)
     , to_boolean(case when (inter.num_locations_clicked > 0
                          or inter.num_locations_booked > 0
                          or inter.total_attr_bookings > 0
                          or inter.num_locations_pv > 0)
                          then 1 else 0 end)                            as user_clicked_a_location
     , sum(case when inter.num_locations_booked > 0 then 1 else 0 end)  as num_acc_bookers
     , sum(case when inter.total_attr_bookings > 0 then 1 else 0 end)   as num_attr_bookers
     , to_boolean(case when inter.num_locations_booked > 0
                       or inter.total_attr_bookings > 0
                  then 1 else 0 end)                                    as user_booked_a_location
     , sum(inter.click_count)                                           as clicks_in_ad_geo
     , sum(inter.num_locations_clicked)                                 as commerce_location_clicks
     , sum(inter.pv_count)                                              as clicks_pvs_in_ad_geo
     , sum(inter.num_locations_pv)                                      as distinct_locations_pv_in_geo
     , sum(inter.hotel_bookings)                                        as hotel_bookings -- formerly ib_count
     , sum(inter.num_locations_booked)                                  as distinct_locations_booked_in_geo
     , sum(inter.accomodation_click_count)                              as accommodation_click_count
     , sum(inter.accomodation_pv_count)                                 as accommodation_pv_count
     , sum(inter.num_distinct_accomodations_clicked)                    as num_distinct_accommodations_clicked
     , sum(inter.num_distinct_accomodations_viewed)                     as num_distinct_accomodations_viewed
     , sum(inter.ATTRACTION_PV_COUNT)                                   as attraction_pv_count
     , sum(inter.NUM_DISTINCT_ATTRACTIONS_VIEWED)                       as num_distinct_attractions_viewed
     , sum(inter.EATERY_CLICK_COUNT)                                    as eatery_click_count
     , sum(inter.EATERY_PV_COUNT)                                       as eatery_pv_count
     , sum(inter.NUM_DISTINCT_EATERIES_CLICKED)                         as num_distinct_eateries_clicked
     , sum(inter.NUM_DISTINCT_EATERIES_VIEWED)                          as num_distinct_eateries_viewed
     , sum(inter.other_click_count)                                     as other_click_count
     , sum(inter.num_distinct_other_locations_clicked)                  as num_distinct_other_locations_clicked
     , sum(inter.total_acc_nights_booked)                               as total_acc_nights_booked
     , round(avg(inter.avg_acc_nightly_spend),2)                        as avg_acc_nightly_spend
     , round(avg(inter.avg_acc_boooking_guests), 1)                     as avg_acc_boooking_guests
     , round(avg(inter.avg_acc_booking_rooms),1)                        as avg_acc_booking_rooms
     , sum(inter.total_attr_bookings)                                   as total_attr_bookings
     , round(avg(inter.avg_attr_spend),2)                               as avg_attr_spend
     , round(avg(inter.avg_attr_guests),1)                              as avg_attr_guests
FROM interactions inter
    LEFT JOIN impressions imp                               on imp.unique_id = inter.unique_id
                                                            and imp.advertiser_id = inter.advertiser_id
                                                            and (imp.last_imp between '&{start_dt_m90}' and '&{end_dt}')
    JOIN (select distinct dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.ADVERTISER_ID = inter.advertiser_id
    LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on inter.ADVERTISER_ID = adcat.id
    LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on inter.advertiser_id = adname.advertiser_id
    JOIN (select unique_id, max(os_type) as os_type, max(locale) as locale, max(user_ip_location_id) as user_location_id
          from  USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds between '&{start_dt_m30}' and '&{end_dt}'
          group by 1
        ) uu on uu.unique_id = inter.unique_id
    LEFT JOIN tripdna.revops.dna_geo_hierarchy reg          on uu.USER_LOCATION_ID = reg.GEO_ID -- user's geo
WHERE inter.ds  between '&{start_dt}' and '&{end_dt}'
group by inter.ds,
     uu.OS_TYPE,
     os_group,
     uu.LOCALE,
     reg.COUNTRY_NAME,
     reg.REGION1_NAME,
     inter.ADVERTISER_ID,
     adname.ADVERTISER_NAME,
     adname.AD_NAME_FORMATTED,
     adcat.ADVERTISER_LABELS,
     inter.UNIQUE_ID is not null,
     imp.UNIQUE_ID is not null,
     user_clicked_a_location,
     user_booked_a_location,
     imp.last_imp,
     daydif
;

commit;
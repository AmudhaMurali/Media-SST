------------------------------------------------------------------
-- uses interactions as base table and joins to impressions to see
-- if those who have interacted in an advertiser's geo have been
-- exposed to an add. Joins to oxford economic data to calculate
-- total TripAdvisor economic impact to the country.
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.mei_economic_impact_ta_only
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.mei_economic_impact_ta_only
WITH impressions as (
        select max(ds)                as last_imp,
               unique_id,
               advertiser_id,
               sum(VIEWABLE_IMPRESSION_COUNTS)  as viewable_ad_impressions,
               sum(VIEWABLE_CLICK_COUNTS)       as viewable_ad_clicks
        from display_ads.sales.user_order_impressions_daily
        where ds between '&{start_dt_m90}' and '&{end_dt}'
        group by unique_id, advertiser_id
    ),
    ad_country_mapping as (
        SELECT distinct lt.country_id, lt.country_primaryname, agm.advertiser_id
        FROM tripdna.uni.location_tree lt
        join (select distinct geo_id, dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.geo_id = lt.geo_id
    )
SELECT int.ds                                                                               as ds,
       uu.OS_TYPE                                                                           as os_type,
       (case when uu.os_type in ('android_browser','iphone_browser','other_phone') then 'Mobile Web'
            when uu.os_type in ('android_native_app','ipad_native_app','iphone_native_app') then 'Native App'
            when uu.os_type in ('android_tablet_browser','ipad_browser','other_tablet') then 'Tablet Web'
            when uu.os_type in ('linux','osx','other', 'windows') then 'Desktop'
            else 'Other' end)                                                               as os_group,
       count(distinct int.UNIQUE_ID)                                                        as uniques,
       imp.UNIQUE_ID is not null                                                            as saw_campaign,
       imp.last_imp                                                                         as imp_ds,
       datediff(day, imp.last_imp, int.ds)                                                  as daydif,
       int.ADVERTISER_ID                                                                    as advertiser_id,
       adname.ADVERTISER_NAME                                                               as advertiser_name,
       adname.AD_NAME_FORMATTED                                                             as ad_name_formatted,
       adcat.ADVERTISER_LABELS                                                              as advertiser_category,
       uu.commerce_country_id                                                               as user_country_id,
       ugeo.country_primaryname                                                             as user_country_name,
       ctr.region                                                                           as user_market,
       adloc.country_id                                                                     as ad_country_id,
       (case when adloc.country_id = uu.commerce_country_id then 'Domestic' else 'Foreign' end) as traveler_type,
       ------- Accommodation Bookings -------
      to_boolean(case when int.hotel_bookings > 0 then 1 else 0 end)                        as booked_acc,
      sum(int.hotel_bookings)                                                               as acc_bookings,      -- total number of accommodation bookings
      sum(int.num_locations_booked)                                                         as num_acc_booked,          -- total different accommodattion locations booked
      sum(int.total_nights_booked)                                                          as total_nights_booked,     -- total nights booked at accommodations
      round(avg(int.avg_nightly_booking_spend),2)                                           as avg_nightly_booking_rate,
      round(avg(int.avg_num_booking_guests),2)                                              as avg_num_booking_guests,
      round(avg(int.avg_num_booking_rooms),2)                                               as avg_num_booking_rooms,
      (case when booked_acc = 1 then round((sum(int.total_nights_booked)/sum(int.hotel_bookings)),2) else 0 end) as avg_nights_per_booking,
      ------- Accommodation Clicks (Meta and IB) -------
      sum(int.accomodation_click_count)                                                     as acc_clicks,
      ------- Attractions Bookings -------
      to_boolean(case when int.num_attr_bookings > 0 then 1 else 0 end)                     as booked_attr,
      sum(int.num_attr_bookings)                                                            as attr_bookings,
      round(avg(int.avg_attr_gross_spend_usd),2)                                            as avg_attr_spend,
      round(avg(int.avg_attr_num_guests),1)                                                 as avg_attr_guests
FROM display_ads.sales.USER_ADVERTISER_INTERACTIONS_DAILY int
    LEFT JOIN impressions imp                                               on imp.UNIQUE_ID = int.UNIQUE_ID
                                                                            and imp.ADVERTISER_ID = int.ADVERTISER_ID
                                                                            and (imp.last_imp between '&{start_dt_m90}' and '&{end_dt}')
    JOIN (select distinct dfp_advertiser_id as advertiser_id from display_ads.sales.gdoc_advertiser_geo_mapping) agm on agm.ADVERTISER_ID = int.advertiser_id
    LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat                  on int.ADVERTISER_ID = adcat.ID
    LEFT JOIN &{pipeline_schema}.dfp_ad_name adname                         on int.advertiser_id = adname.advertiser_id
    JOIN ad_country_mapping adloc                                      on adloc.advertiser_id = int.advertiser_id
    JOIN (select unique_id,
        max(os_type) as os_type,
        max(locale) as locale,
        max(user_ip_location_id) as user_location_id,
        max(c.country_id) as commerce_country_id,
        max(user_ip_country_name) as user_country_name
          from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS u
              left join rio_sf.public.country_id_to_country c on c.COUNTRY=u.USER_IP_COUNTRY_NAME
          where ds between '&{start_dt_m30}' and '&{start_dt}'
          group by 1) uu on uu.unique_id = int.unique_id
    LEFT JOIN (SELECT distinct country_id, country_primaryname FROM tripdna.uni.location_tree) ugeo on ugeo.country_id = uu.commerce_country_id
    LEFT JOIN rio_sf.anm.country_to_region ctr on ugeo.country_primaryname = ctr.country
WHERE int.ds between '&{start_dt}' and '&{end_dt}'
GROUP BY int.ds,
        uu.os_type,
        os_group,
        saw_campaign,
        imp.last_imp,
        daydif,
        int.ADVERTISER_ID,
        adname.ADVERTISER_NAME,
        adname.AD_NAME_FORMATTED,
        adcat.ADVERTISER_LABELS,
        uu.commerce_country_id,
        ugeo.country_primaryname,
        ctr.region,
        adloc.country_id,
        traveler_type,
        booked_acc,
        booked_attr

;

commit;
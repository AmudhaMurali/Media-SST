------------------------------------------------------------------
-- uses interactions as base table and joins to impressions to see
-- if those who have interacted in an advertiser's geo have been
-- exposed to an add. Joins to oxford economic data to calculate
-- total TripAdvisor economic impact to the country.
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.mei_economic_impact
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.mei_economic_impact
WITH impressions as (
        select max(ds)                as last_imp,
               unique_id,
               advertiser_id,
               sum(VIEWABLE_IMPRESSION_COUNTS)  as viewable_ad_impressions,
               sum(VIEWABLE_CLICK_COUNTS)       as viewable_ad_clicks
        from display_ads.sales.user_order_impressions_daily
        where ds between '&{start_dt_m90}' and '&{end_dt}'
        group by unique_id, advertiser_id
    )
SELECT int.ds                                                                               as ds,
       uu.OS_TYPE                                                                           as os_type,
       (case when uu.os_type in ('android_browser','iphone_browser','other_phone')
              then 'Mobile Web'
            when uu.os_type in ('android_native_app','ipad_native_app','iphone_native_app')
              then 'Native App'
            when uu.os_type in ('android_tablet_browser','ipad_browser','other_tablet')
              then 'Tablet Web'
            when uu.os_type in ('linux','osx','other', 'windows')
               then 'Desktop'
            else 'Other' end)                                                               as os_group,
       count(distinct int.UNIQUE_ID)                                                        as uniques,
       imp.UNIQUE_ID is not null                                                            as saw_campaign,
       sum(case when imp.UNIQUE_ID is not null then 1 else 0 end)                           as num_saw_campaign,
       imp.last_imp                                                                         as imp_ds,
       datediff(day, imp.last_imp, int.ds)                                                  as daydif,
       int.ADVERTISER_ID                                                                    as advertiser_id,
       adname.ADVERTISER_NAME                                                               as advertiser_name,
       adname.AD_NAME_FORMATTED                                                             as ad_name_formatted,
       adcat.ADVERTISER_LABELS                                                              as advertiser_category,
       ox.ox_name                                                                           as advertiser_country,
       ugeo.country_name                                                                    as user_country,
       ctr.region                                                                           as user_market,
       (case when ox.ox_name = ugeo.country_name then 'Domestic' else 'Foreign' end)        as traveler_type,

       ------- Accommodation Bookings -------
      to_boolean(case when int.hotel_bookings > 0 then 1 else 0 end)                        as booked_acc,
      sum(int.hotel_bookings)                                                               as total_acc_bookings,      -- total number of accommodation bookings
      sum(int.num_locations_booked)                                                         as num_acc_booked,          -- total different accommodattion locations booked
      sum(int.total_nights_booked)                                                          as total_nights_booked,     -- total nights booked at accommodations
      round(avg(int.avg_nightly_booking_spend),2)                                           as avg_nightly_booking_rate,
      round(avg(int.avg_num_booking_guests),2)                                              as avg_num_booking_guests,
      round(avg(int.avg_num_booking_rooms),2)                                               as avg_num_booking_rooms,
      (case when booked_acc = 1 then round((sum(int.total_nights_booked)/sum(int.hotel_bookings)),2) else 0 end) as avg_nights_per_booking,

      ------- Accommodation Clicks (Meta and IB) -------
      sum(int.accomodation_click_count)                                                     as num_acc_clicks,
      round(avg(int.avg_nightly_click_spend),2)                                             as avg_nightly_click_rate,
      round(avg(int.avg_num_click_guests),2)                                                as avg_num_click_guests,
      round(avg(int.avg_num_click_rooms),2)                                                 as avg_num_click_rooms,
      --round((case when booked_acc = 1 then (num_acc_booked/num_acc_clicks) else 0 end),2)   as frac_clicks_booked,     -- fraction (percent) of total meta/ib clicks that were booked

      ------- Attractions Bookings -------
      to_boolean(case when int.num_attr_bookings > 0 then 1 else 0 end)                     as booked_attr,
      sum(int.num_attr_bookings)                                                            as attr_bookings,
      round(avg(int.avg_attr_gross_spend_usd),2)                                            as avg_attr_spend,
      round(avg(int.avg_attr_num_guests),1)                                                 as avg_attr_guests,

       ------- Oxford Economic Daily Spend per Country -------
       oxt.ACCOMMDATION                                                                     as t_acc,       -- oxford economic: daily accomodation spend - total
       oxt.FOOD_BEV                                                                         as t_food,      -- oxford economic: daily food/bev spend - total
       oxt.TRANSPORTATION                                                                   as t_transport, -- oxford economic: daily transportation spend - total
       oxt.RETAIL                                                                           as t_retail,    -- oxford economic: daily retail spend - total
       oxt.REC_CULTURE_SPORTS                                                               as t_rec,       -- oxford economic: daily rec/culture/sports spend - total
       oxt.OTHER                                                                            as t_other,     -- oxford economic: daily other spend - total
       oxt.TOTAL                                                                            as t_total,     -- oxford economic: daily total spend - total
       oxd.ACCOMMDATION                                                                     as d_acc,       -- oxford economic: daily accomodation spend - domestic
       oxd.FOOD_BEV                                                                         as d_food,      -- oxford economic: daily food/bev spend - domestic
       oxd.TRANSPORTATION                                                                   as d_transport, -- oxford economic: daily transportation spend - domestic
       oxd.RETAIL                                                                           as d_retail,    -- oxford economic: daily retail spend - domestic
       oxd.REC_CULTURE_SPORTS                                                               as d_rec,       -- oxford economic: daily rec/culture/sports spend - domestic
       oxd.OTHER                                                                            as d_other,     -- oxford economic: daily other spend - domestic
       oxd.TOTAL                                                                            as d_total,     -- oxford economic: daily total spend - domestic
       oxf.ACCOMMDATION                                                                     as f_acc,       -- oxford economic: daily accomodation spend - foreign
       oxf.FOOD_BEV                                                                         as f_food,      -- oxford economic: daily food/bev spend - foreign
       oxf.TRANSPORTATION                                                                   as f_transport, -- oxford economic: daily transportation spend - foreign
       oxf.RETAIL                                                                           as f_retail,    -- oxford economic: daily retail spend - foreign
       oxf.REC_CULTURE_SPORTS                                                               as f_rec,       -- oxford economic: daily rec/culture/sports spend - foreign
       oxf.OTHER                                                                            as f_other,     -- oxford economic: daily other spend - foreign
       oxf.TOTAL                                                                            as f_total      -- oxford economic: daily total spend - foreign
FROM display_ads.sales.USER_ADVERTISER_INTERACTIONS_DAILY int
    LEFT JOIN impressions imp                                               on imp.UNIQUE_ID = int.UNIQUE_ID
                                                                            and imp.ADVERTISER_ID = int.ADVERTISER_ID
                                                                            and (imp.last_imp between '&{start_dt_m90}' and '&{end_dt}')
    LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat                  on int.ADVERTISER_ID = adcat.ID
    LEFT JOIN &{pipeline_schema}.dfp_ad_name adname                         on int.advertiser_id = adname.advertiser_id
    LEFT JOIN (select distinct advertiser_id, ox_loc_id, ox_name
               from &{pipeline_schema}.advertiser_oxford_loc_mapping) ox  on int.ADVERTISER_ID = ox.advertiser_id
    JOIN (select unique_id,
        max(os_type) as os_type,
        max(locale) as locale,
        max(user_ip_location_id) as user_location_id,
        max(c.country_id) as commerce_country_id,
        max(user_ip_country_name) as user_country_name
          from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS u
              left join rio_sf.public.country_id_to_country c on c.COUNTRY=u.USER_IP_COUNTRY_NAME
          where ds between '&{start_dt_m30}' and '&{start_dt}'
          group by 1
        ) uu on uu.unique_id = int.unique_id
    LEFT JOIN tripdna.revops.dna_geo_hierarchy ugeo                         on uu.commerce_country_id = ugeo.geo_id
    left join rio_sf.anm.country_to_region ctr                              on ugeo.country_name = ctr.country
    left join rio_sf.SOCIAL.MEI_OXFORD_ECONOMICS_TOTAL oxt                       on ox.ox_name = oxt.COUNTRY
    left join rio_sf.SOCIAL.MEI_OXFORD_ECONOMICS_DOMESTIC oxd                    on ox.ox_name = oxd.COUNTRY
    left join rio_sf.SOCIAL.MEI_OXFORD_ECONOMICS_FOREIGN oxf                     on ox.ox_name = oxf.COUNTRY
WHERE int.ds between '&{start_dt}' and '&{end_dt}'
AND adname.ADVERTISER_NAME is not null
AND ox.ox_name is not null
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
        ox.ox_name,
        ugeo.country_name,
        ctr.region,
        traveler_type,
        booked_acc,
        booked_attr,
       oxt.accommdation, oxt.FOOD_BEV, oxt.TRANSPORTATION, oxt.RETAIL, oxt.REC_CULTURE_SPORTS, oxt.other, oxt.TOTAL,
       oxd.accommdation, oxd.FOOD_BEV, oxd.TRANSPORTATION, oxd.RETAIL, oxd.REC_CULTURE_SPORTS, oxd.other, oxd.TOTAL,
       oxf.accommdation, oxf.FOOD_BEV, oxf.TRANSPORTATION, oxf.RETAIL, oxf.REC_CULTURE_SPORTS, oxf.other, oxf.TOTAL
;

commit;
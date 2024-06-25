-- this table is associates the booking to each advertiser


begin;
delete from &{pipeline_schema}.booking_distance_adv
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.booking_distance_adv
SELECT bdg.ds                                                                           as ds,
       ag.advertiser_id                                                                 as advertiser_id,
       adname.ADVERTISER_NAME                                                           as advertiser_name,
       adname.AD_NAME_FORMATTED                                                         as ad_name_formatted,
       adcat.ADVERTISER_LABELS                                                          as advertiser_category,
       bdg.user_geo_id                                                                  as user_geo_id,
       ugeo.country_id                                                                  as user_country_id,
       ugeo.country_primaryname                                                         as user_country_name,
       ctr.region                                                                       as user_market,
       bdg.distance_traveler                                                            as distance_traveler,
       bdg.short_distance_tvlr                                                          as short_distance_tvlr,
       bdg.long_distance_tvlr                                                           as long_distance_tvlr,
       bdg.intl_traveler                                                                as intl_traveler,
       count(distinct bdg.unique_id)                                                    as uniques,
       count(distinct(case when bdg.acc_bookings >= 1 then unique_id else null end))    as acc_bookers,
       count(distinct(case when bdg.attr_bookings >= 1 then unique_id else null end))   as attr_bookers,
       sum(bdg.acc_bookings)                                                            as acc_bookings,
       sum(bdg.total_nights_booked)                                                     as total_nights_booked,
       (sum(bdg.total_nights_booked * bdg.acc_bookings) / sum(bdg.acc_bookings))        as acc_nights_per_booking,
       (sum(coalesce(bdg.avg_acc_rooms,1) * bdg.acc_bookings) / sum(bdg.acc_bookings))  as acc_w_avg_rooms,
       (sum(bdg.avg_acc_guests * bdg.acc_bookings) / sum(bdg.acc_bookings))             as acc_w_avg_guests,
       (sum(bdg.avg_nightly_spend * bdg.acc_bookings) / sum(bdg.acc_bookings))          as acc_w_avg_spend,
       (sum(bdg.avg_days_out * bdg.acc_bookings) / sum(bdg.acc_bookings))               as w_avg_booking_window,
        sum(bdg.attr_bookings)                                                          as attr_bookings,
       (sum(bdg.avg_attr_spend * bdg.attr_bookings) / sum(bdg.attr_bookings))           as attr_w_avg_spend,
       (sum(bdg.avg_attr_guests * bdg.attr_bookings) / sum(bdg.attr_bookings))          as attr_w_avg_guests
FROM rio_sf.cx_analytics.booking_distance_geo bdg
JOIN (SELECT DISTINCT geo_id, dfp_advertiser_id as advertiser_id from  display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.geo_id = bdg.booking_geo_id
LEFT JOIN tripdna.uni.location_tree ugeo on ugeo.location_id = bdg.user_geo_id
LEFT JOIN rio_sf.display_sales.f_dfp_advertisers adcat on ag.ADVERTISER_ID = adcat.id
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on ag.advertiser_id = adname.advertiser_id
LEFT JOIN rio_sf.anm.country_to_region ctr on ugeo.country_primaryname = ctr.country
WHERE bdg.ds between '&{start_dt}' and '&{end_dt}'
GROUP BY bdg.ds, ag.advertiser_id, adname.ADVERTISER_NAME, adname.AD_NAME_FORMATTED,
         adcat.ADVERTISER_LABELS, bdg.user_geo_id, ugeo.country_id, ugeo.country_primaryname, ctr.region,
         bdg.distance_traveler, bdg.short_distance_tvlr, bdg.long_distance_tvlr, bdg.intl_traveler
;

commit;
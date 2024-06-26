-- Every day takes the bookings (and booking detail) for the ad geo and the average across the entire competitive set


begin;
delete from &{pipeline_schema}.ci_daily_comp_bookings
where ds = '&{start_dt}';

INSERT INTO &{pipeline_schema}.ci_daily_comp_bookings
WITH geo_bookings as (
        SELECT b.ds,
               loc.geo_id                                                                                           AS geo_id,
               count(distinct b.unique_id)                                                                          AS uniques,
               count(distinct(case when b.simple_placetype_name = 'Accomodation' then b.unique_id else null end))   AS acc_bookers,
               count(distinct(case when b.simple_placetype_name = 'Attraction' then b.unique_id else null end))     AS attr_bookers,
               ((acc_bookers+attr_bookers)-uniques)                                                                 AS bookers_both,
               sum(b.total_bookings)                                                                                AS acc_bookings,
               sum(b.total_nights_booked)                                                                           AS acc_total_nights_booked,
               (sum(total_nights_booked * total_bookings) / sum(total_bookings))                                    AS acc_nights_per_booking,
               (sum(coalesce(avg_num_rooms,1) * total_bookings) / sum(total_bookings))                              AS acc_w_avg_rooms,
               (sum(avg_nightly_spend * total_bookings) / sum(total_bookings))                                      AS acc_w_avg_spend,
               avg(case when b.simple_placetype_name = 'Accomodation' then b.avg_days_out else null end)            AS acc_avg_booking_window,
               sum(b.num_attr_bookings)                                                                             AS attr_bookings,
               (sum(avg_attr_gross_spend_usd * num_attr_bookings) / sum(num_attr_bookings))                         AS attr_w_avg_spend,
               avg(case when b.simple_placetype_name = 'Attraction' then b.avg_days_out else null end)              AS attr_avg_booking_window
        from DISPLAY_ADS.sales.vw_user_location_interactions_daily b
        join (select distinct unique_id from rio_sf.rust.a_unique_users auu where ds between '&{start_dt_m30}' and '&{start_dt}') auu on b.unique_id = auu.unique_id
        left join &{pipeline_schema}.geo_to_location loc on b.location_id = loc.location_id
        where b.action_type = 'booking'
        and b.ds = '&{start_dt}'
        group by loc.geo_id, b.ds
        )
SELECT ba.ds                                                AS ds,
       s.advertiser_id                                      AS advertiser_id,
       s.advertiser_name                                    AS advertiser_name,
       s.ad_name_formatted                                  AS ad_name_formatted,
       s.ad_geo_id                                          AS ad_geo_id,
       s.ad_geo_name                                        AS ad_geo_name,
       cast(median(ba.uniques) as int)                      AS ag_unique_bookers, -- advertiser geo unique bookers
       cast(median(ba.acc_bookers) as int)                  AS ag_acc_bookers,
       cast(median(ba.attr_bookers) as int)                 AS ag_attr_bookers,
       cast(median(ba.bookers_both) as int)                 AS ag_bookers_both,
       cast(median(ba.acc_bookings) as int)                 AS ag_acc_bookings,
       cast(median(ba.acc_total_nights_booked) as int)      AS ag_acc_nights_booked,
       round(median(ba.acc_nights_per_booking),2)           AS ag_nights_per_booking,
       round(median(ba.acc_w_avg_rooms),2)                  AS ag_rooms_per_booking,
       round(median(ba.acc_w_avg_spend),2)                  AS ag_acc_w_avg_spend,
       round(median(ba.acc_avg_booking_window),2)           AS ag_acc_avg_booking_window,
       cast(median(ba.attr_bookings) as int)                AS ag_attr_bookings,
       round(median(ba.attr_w_avg_spend),2)                 AS ag_attr_w_avg_spend,
       round(median(ba.attr_avg_booking_window),2)          AS ag_attr_avg_booking_window,
       cast(avg(bs.uniques) as int)                         AS cs_unique_bookers, -- competitive set unique bookers
       cast(avg(bs.acc_bookers) as int)                     AS cs_acc_bookers, -- competitive set acc bookers
       cast(avg(bs.attr_bookers) as int)                    AS cs_attr_bookers,
       cast(avg(bs.bookers_both) as int)                    AS cs_bookers_both,
       cast(avg(bs.acc_bookings) as int)                    AS cs_acc_bookings,
       cast(avg(bs.acc_total_nights_booked) as int)         AS cs_acc_nights_booked,
       round(avg(bs.acc_nights_per_booking),2)              AS cs_nights_per_booking,
       round(avg(bs.acc_w_avg_rooms),2)                     AS cs_rooms_per_booking,
       round(avg(bs.acc_w_avg_spend),2)                     AS cs_acc_w_avg_spend,
       round(avg(bs.acc_avg_booking_window),2)              AS cs_acc_avg_booking_window,
       cast(avg(bs.attr_bookings) as int)                   AS cs_attr_bookings,
       round(avg(bs.attr_w_avg_spend),2)                    AS cs_attr_w_avg_spend,
       round(avg(bs.attr_avg_booking_window),2)             AS cs_attr_avg_booking_window
FROM &{pipeline_schema}.mei_ad_competitive_geo_set s
LEFT JOIN geo_bookings ba on s.ad_geo_id = ba.geo_id
LEFT JOIN geo_bookings bs on s.similar_geo_id = bs.geo_id and bs.ds = ba.ds
and ba.ds = '&{start_dt}'
group by s.advertiser_id, s.advertiser_name, s.ad_name_formatted, s.ad_geo_id, s.ad_geo_name, ba.ds
;

commit;
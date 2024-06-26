--- for each ad geo and competitive geo in the ML output (mei_competitive_geo_set)
--- this table returns the number of bookings, avg booking price, etc. for the geos

-- this table will replace itself every day with data from the past 3 months (90d) --

begin;
delete from &{pipeline_schema}.ci_ad_geo_bookings_2;

INSERT INTO &{pipeline_schema}.ci_ad_geo_bookings_2
WITH geo_bookings as (
        SELECT loc.geo_id                                                                                           AS geo_id,
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
        left join &{pipeline_schema}.geo_to_location loc on b.location_id = loc.location_id
        where b.action_type = 'booking'
        and b.ds between '&{start_dt_m90}' and '&{start_dt}'
        group by loc.geo_id
        ),
    geo_pv_clicks as (
        SELECT l.geo_id                                                                                             AS geo_id,
               count(distinct(case when v.action_type = 'pageviews' then v.unique_id else null end))                AS uu_pv,
               sum(case when v.action_type = 'pageviews' then v.action_count else null end)                         AS pvs,
               count(distinct(case when v.action_type = 'click' then v.unique_id else null end))                    AS uu_cc,
               sum(case when v.action_type = 'click' then v.action_count else null end)                             AS commerce_clicks,
               count(distinct v.unique_id)                                                                          AS uniques
        from DISPLAY_ADS.sales.vw_user_location_interactions_daily v
        left join &{pipeline_schema}.geo_to_location l on v.location_id = l.location_id
        where v.ds between '&{start_dt_m90}' and '&{start_dt}'
        group by l.geo_id
    )
SELECT s.advertiser_id                  AS advertiser_id,
       s.advertiser_name                AS advertiser_name,
       s.ad_name_formatted              AS ad_name_formatted,
       s.ad_geo_id                      AS ad_geo_id,
       s.ad_geo_name                    AS ad_geo_name,
       ca.uniques                       AS ag_uniques,
       ba.uniques                       AS ag_uu_bookers,
       ba.acc_bookers                   AS ag_acc_bookers,
       ba.attr_bookers                  AS ag_attr_bookers,
       ba.bookers_both                  AS ag_bookers_both,
       ba.acc_bookings                  AS ag_acc_bookings,
       ba.acc_total_nights_booked       AS ag_acc_nights_booked,
       ba.acc_nights_per_booking        AS ag_nights_per_booking,
       ba.acc_w_avg_rooms               AS ag_rooms_per_booking,
       ba.acc_w_avg_spend               AS ag_acc_w_avg_spend,
       ba.acc_avg_booking_window        AS ag_acc_avg_booking_window,
       ba.attr_bookings                 AS ag_attr_bookings,
       ba.attr_w_avg_spend              AS ag_attr_w_avg_spend,
       ba.attr_avg_booking_window       AS ag_attr_avg_booking_window,
       ca.uu_pv                         AS ag_uu_pv,
       ca.pvs                           AS ag_pvs,
       ca.uu_cc                         AS ag_uu_cc,
       ca.commerce_clicks               AS ag_commerce_clicks,
       s.rank                           AS comp_rank,
       s.similar_geo_id                 AS comp_geo_id,
       s.similar_geo_name               AS comp_geo_name,
       cs.uniques                       AS cg_uniques,
       bs.uniques                       AS cg_geo_uu_bookers,
       bs.acc_bookers                   AS cg_acc_bookers,
       bs.attr_bookers                  AS cg_attr_bookers,
       bs.bookers_both                  AS cg_bookers_both,
       bs.acc_bookings                  AS cg_acc_bookings,
       bs.acc_total_nights_booked       AS cg_acc_nights_booked,
       bs.acc_nights_per_booking        AS cg_nights_per_booking,
       bs.acc_w_avg_rooms               AS cg_rooms_per_booking,
       bs.acc_w_avg_spend               AS cg_acc_w_avg_spend,
       bs.acc_avg_booking_window        AS cg_acc_avg_booking_window,
       bs.attr_bookings                 AS cg_attr_bookings,
       bs.attr_w_avg_spend              AS cg_attr_w_avg_spend,
       bs.attr_avg_booking_window       AS cg_attr_avg_booking_window,
       cs.uu_pv                         AS cg_uu_pv,
       cs.pvs                           AS cg_pvs,
       cs.uu_cc                         AS cg_uu_cc,
       cs.commerce_clicks               AS cg_commerce_clicks
FROM &{pipeline_schema}.mei_ad_competitive_geo_set s
LEFT JOIN geo_bookings ba on s.ad_geo_id = ba.geo_id
LEFT JOIN geo_bookings bs on s.similar_geo_id = bs.geo_id
LEFT JOIN geo_pv_clicks ca on s.ad_geo_id = ca.geo_id
LEFT JOIN geo_pv_clicks cs on s.similar_geo_id = cs.geo_id
;

commit;
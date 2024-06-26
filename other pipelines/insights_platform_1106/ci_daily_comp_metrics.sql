-- Every day takes the clicks, pv's, uniques and bookings for the ad geo and the average across the entire competitive set


begin;
delete from &{pipeline_schema}.ci_daily_comp_metrics
where ds between '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.ci_daily_comp_metrics
with geo_pv_clicks as (
        SELECT v.ds,
               l.geo_id                                                                                             AS geo_id,
               (case when gc.country_name = auu.user_country_name then 'Domestic' else 'Foreign' end)               AS tvlr_type,
               count(distinct(case when v.action_type = 'pageviews' then v.unique_id else null end))                AS uu_pv,
               sum(case when v.action_type = 'pageviews' then v.action_count else null end)                         AS pvs,
               count(distinct(case when v.action_type = 'click' then v.unique_id else null end))                    AS uu_cc,
               sum(case when v.action_type = 'click' then v.action_count else null end)                             AS commerce_clicks,
               sum(case when v.action_type = 'booking' and v.simple_placetype_name = 'Accomodation'
                   then v.action_count else null end)                                                               AS acc_bookings,
               sum(case when v.action_type = 'booking' and v.simple_placetype_name = 'Accomodation'
                   then v.total_nights_booked else null end)                                                        AS acc_total_nights_booked,
               round(avg(case when v.action_type = 'booking' and v.simple_placetype_name = 'Accomodation'
                   then v.avg_num_rooms else null end),2)                                                           AS acc_avg_rooms,
               round(avg(case when v.action_type = 'booking' and v.simple_placetype_name = 'Accomodation'
                   then v.avg_nightly_spend else null end),2)                                                       AS acc_avg_spend,
               round(avg(case when v.action_type = 'booking' and v.simple_placetype_name = 'Accomodation'
                   then v.avg_days_out else null end),2)                                                            AS acc_avg_booking_window,
               round(avg(case when v.action_type = 'click' and v.simple_placetype_name = 'Accomodation'
                   then v.avg_days_out else null end),2)                                                            AS acc_avg_click_window,
               sum(case when v.action_type = 'booking' and v.simple_placetype_name = 'Attraction'
                   then v.action_count else null end)                                                               AS attr_bookings,
               round(avg(case when v.action_type = 'booking' and v.simple_placetype_name = 'Attraction'
                   then v.avg_attr_gross_spend_usd else null end),2)                                                AS attr_avg_spend,
               round(avg(case when v.action_type = 'booking' and v.simple_placetype_name = 'Attraction'
                   then v.avg_days_out else null end),2)                                                            AS attr_avg_booking_window,
               count(distinct v.unique_id)                                                                          AS uniques
        from DISPLAY_ADS.sales.vw_user_location_interactions_daily v
        join (select distinct unique_id, max(user_ip_country_name) as user_country_name
              from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS auu where ds between '&{start_dt_m30}' and '&{end_dt}' group by 1) auu on v.unique_id = auu.unique_id
        left join &{pipeline_schema}.geo_to_location l on v.location_id = l.location_id
        join tripdna.revops.dna_geo_hierarchy gc on gc.geo_id = l.geo_id
        where v.ds between '&{start_dt}' and '&{end_dt}'
        group by l.geo_id, v.ds, tvlr_type
    )
SELECT ca.ds                                                AS ds,
       s.advertiser_id                                      AS advertiser_id,
       s.advertiser_name                                    AS advertiser_name,
       s.ad_name_formatted                                  AS ad_name_formatted,
       s.ad_geo_id                                          AS ad_geo_id,
       s.ad_geo_name                                        AS ad_geo_name,
       ca.tvlr_type                                         AS tvlr_type,
       cast(median(ca.uniques) as int)                      AS ag_uniques,
       cast(median(ca.uu_pv) as int)                        AS ag_uu_pv,
       cast(median(ca.pvs) as int)                          AS ag_pvs,
       cast(median(ca.uu_cc) as int)                        AS ag_uu_cc,
       cast(median(ca.commerce_clicks) as int)              AS ag_commerce_clicks,
       cast(median(ca.acc_bookings) as int)                 AS ag_acc_bookings,
       cast(median(ca.acc_total_nights_booked) as int)      AS ag_acc_nights_booked,
       cast(median(ca.acc_avg_rooms) as double)             AS ag_acc_rooms_booked,
       cast(median(ca.acc_avg_spend) as double)             AS ag_acc_avg_spend,
       cast(median(ca.acc_avg_booking_window) as double)    AS ag_acc_avg_booking_window,
       cast(median(ca.acc_avg_click_window) as double)      AS ag_acc_avg_click_window,
       cast(median(ca.attr_bookings) as int)                AS ag_attr_bookings,
       cast(median(ca.attr_avg_spend) as double)            AS ag_attr_avg_spend,
       cast(median(ca.attr_avg_booking_window) as double)   AS ag_attr_avg_booking_window,
       cast(avg(cs.uniques) as int)                         AS cs_uniques,
       cast(avg(cs.uu_pv) as int)                           AS cs_uu_pv,
       cast(avg(cs.pvs) as int)                             AS cs_pvs,
       cast(avg(cs.uu_cc) as int)                           AS cs_uu_cc,
       cast(avg(cs.commerce_clicks) as int)                 AS cs_commerce_clicks,
       cast(avg(cs.acc_bookings) as int)                    AS cs_acc_bookings,
       cast(avg(cs.acc_total_nights_booked) as int)         AS cs_acc_nights_booked,
       cast(avg(cs.acc_avg_rooms) as double)                AS cs_acc_rooms_booked,
       cast(avg(cs.acc_avg_spend) as double)                AS cs_acc_avg_spend,
       cast(avg(cs.acc_avg_booking_window) as double)       AS cs_acc_avg_booking_window,
       cast(avg(cs.acc_avg_click_window) as double)         AS cs_acc_avg_click_window,
       cast(avg(cs.attr_bookings) as int)                   AS cs_attr_bookings,
       cast(avg(cs.attr_avg_spend) as double)               AS cs_attr_avg_spend,
       cast(avg(cs.attr_avg_booking_window) as double)      AS cs_attr_avg_booking_window
FROM &{pipeline_schema}.mei_competitive_set s
LEFT JOIN geo_pv_clicks ca on s.ad_geo_id = ca.geo_id
LEFT JOIN geo_pv_clicks cs on s.similar_geo_id = cs.geo_id  and cs.ds = ca.ds and cs.tvlr_type = ca.tvlr_type
and ca.ds between '&{start_dt}' and '&{end_dt}'
group by ca.ds, s.advertiser_id, s.advertiser_name, s.ad_name_formatted, s.ad_geo_id, s.ad_geo_name, ca.tvlr_type
;

commit;
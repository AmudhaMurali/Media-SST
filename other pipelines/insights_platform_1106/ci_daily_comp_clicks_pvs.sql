-- Every day takes the clicks, pv's, uniques and bookings for the ad geo and the average across the entire competitive set


begin;
delete from &{pipeline_schema}.ci_daily_comp_clicks_pvs
where ds = '&{start_dt}';

INSERT INTO &{pipeline_schema}.ci_daily_comp_clicks_pvs
with geo_pv_clicks as (
        SELECT v.ds,
               l.geo_id                                                                                             AS geo_id,
               count(distinct(case when v.action_type = 'pageviews' then v.unique_id else null end))                AS uu_pv,
               sum(case when v.action_type = 'pageviews' then v.action_count else null end)                         AS pvs,
               count(distinct(case when v.action_type = 'click' then v.unique_id else null end))                    AS uu_cc,
               sum(case when v.action_type = 'click' then v.action_count else null end)                             AS commerce_clicks,
               sum(case when v.action_type = 'booking' then v.action_count else null end)                           AS bookings,
               count(distinct v.unique_id)                                                                          AS uniques
        from DISPLAY_ADS.sales.vw_user_location_interactions_daily v
        join (select distinct unique_id from rio_sf.rust.a_unique_users auu where ds between '&{start_dt_m30}' and '&{start_dt}') auu on v.unique_id = auu.unique_id
        left join &{pipeline_schema}.geo_to_location l on v.location_id = l.location_id
        where v.ds = '&{start_dt}'
        group by l.geo_id, v.ds
    )
SELECT ca.ds                                    AS ds,
       s.advertiser_id                          AS advertiser_id,
       s.advertiser_name                        AS advertiser_name,
       s.ad_name_formatted                      AS ad_name_formatted,
       s.ad_geo_id                              AS ad_geo_id,
       s.ad_geo_name                            AS ad_geo_name,
       cast(median(ca.uniques) as int)          AS ag_uniques,
       cast(median(ca.uu_pv) as int)            AS ag_uu_pv,
       cast(median(ca.pvs) as int)              AS ag_pvs,
       cast(median(ca.uu_cc) as int)            AS ag_uu_cc,
       cast(median(ca.commerce_clicks) as int)  AS ag_commerce_clicks,
       cast(median(ca.bookings) as int)         AS ag_bookings,
       cast(avg(cs.uniques) as int)             AS cs_uniques,
       cast(avg(cs.uu_pv) as int)               AS cs_uu_pv,
       cast(avg(cs.pvs) as int)                 AS cs_pvs,
       cast(avg(cs.uu_cc) as int)               AS cs_uu_cc,
       cast(avg(cs.commerce_clicks) as int)     AS cs_commerce_clicks,
       cast(avg(cs.bookings) as int)            AS cs_bookings
FROM &{pipeline_schema}.mei_ad_competitive_geo_set s
LEFT JOIN geo_pv_clicks ca on s.ad_geo_id = ca.geo_id
LEFT JOIN geo_pv_clicks cs on s.similar_geo_id = cs.geo_id  and cs.ds = ca.ds
and ca.ds = '&{start_dt}'
group by ca.ds, s.advertiser_id, s.advertiser_name, s.ad_name_formatted, s.ad_geo_id, s.ad_geo_name
;

commit;

-----------------------------------------------------------
-- remake of the Woodsy Job partner_trip_impressions_v2
-- gives a a count of uniques and dwell time for each
-- sponsored trip
-- this is a final table (i.e. to be used in tableau)
-----------------------------------------------------------

begin;
delete from &{pipeline_schema}.sponsored_trips_dwell_time_wp
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_dwell_time_wp
with    operative_data as (
        select distinct op.op_advertiser_id,
                        op.advertiser_name,
                        op.sales_order_id,
                        op.sales_order_name,
                        op.industry,
                        ac.REGION,
                        op.account_exec
        from display_ads.sales.op1_line_items op
        left join display_ads.sales.op1_advertiser_country ac on ac.op_advertiser_id = op.op_advertiser_id
    )

SELECT dt.ds                                                        AS ds,
       dt.trip_id                                                   AS trip_id,
       trips.trip_title                                             AS trip_title,
       trips.username                                               AS username,
       trips.display_name                                           AS display_name,
       uu.OS_TYPE                                                   AS os_type,
       uu.LOCALE                                                    AS locale,

       count(distinct dt.unique_id)                                 AS uniques,
       sum(dt.sum_dwell_time)                                       AS total_dwell_time,
       sum(dt.samples_under_10_min)                                 AS samples_under_10_min,
       sum(dt.samples_10_to_15_min)                                 AS samples_over_10_min,
       (sum(dt.samples_under_10_min)+sum(dt.samples_10_to_15_min))  AS total_samples
FROM display_ads.public.sponsored_trips_dwell_time_v2 dt
JOIN rio_sf.rust.a_unique_users uu ON lower(uu.unique_id) = lower(dt.unique_id)
                                   AND uu.ds = dt.ds
                                   AND uu.is_blessed = 1
JOIN (
        select sl.user_id               as user_id,
            mm.USERNAME                 as username,
            mm.DISPLAY_NAME             as display_name,
            sl.id                       as trip_id,
            sl.title                    as trip_title,
            sl.description              as trip_desc,
            to_date(sl.created)         as created,
            to_date(sl.first_published) as first_published
        from rio_sf.cx_analytics.t_saves_lists sl
        left join rio_sf.cx_analytics.member_metadata mm on sl.USER_ID = mm.MEMBERID
        WHERE to_date(created)>= '2019-01-01'
        and FIRST_PUBLISHED is not null) trips on trips.trip_id = dt.trip_id
left join &{pipeline_schema}.sponsored_trips st on dt.trip_id = st.trip_id
join (select distinct trip_id, order_id from  &{pipeline_schema}.trips_orderid_mapping ) map on dt.trip_id = map.trip_id
join operative_data op  on map.order_id = op.sales_order_id
WHERE dt.ds between '&{start_dt}' and '&{end_dt}'
AND lower(trips.username) in (select distinct lower(username) from &{pipeline_schema}.trip_sponsors)
GROUP BY dt.ds                ,
         dt.trip_id           ,
         trips.trip_title     ,
         trips.username       ,
         trips.display_name   ,
         uu.OS_TYPE           ,
         uu.LOCALE
;

commit;
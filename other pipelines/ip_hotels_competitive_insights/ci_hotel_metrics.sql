begin;

delete from &{pipeline_schema}.ci_hotel_metrics
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.ci_hotel_metrics

with brand_property_locations as (
    select distinct ahdd.location_id, ahdd.brand_name, aldd.country as property_country_name, hex_encode(lower(ahdd.brand_name)) as brand_code
    from rio_sf.hotels_sst.a_hotel_details_daily ahdd
    join rio_sf.hotels_sst.a_location_details_daily aldd on aldd.location_id = ahdd.location_id and aldd.ds = ahdd.ds and aldd.ds between '&{start_dt}' and '&{end_dt}'
    where ahdd.ds between '&{start_dt}' and '&{end_dt}'
    and ahdd.brand_name is not null)
select v.ds,
       bpl.brand_name,
       bpl.brand_code,
       bpl.property_country_name,
       --auu.user_country_name,
       count(distinct v.unique_id) as uniques,
       cast(sum(case when v.action_type = 'pageviews' then v.action_count else null end) as int) as pageviews,
       cast(sum(case when v.action_type = 'click' then v.action_count else null end )as int) as clicks,
       round(sum(case when v.action_type = 'estimated booking' then v.action_count else null end),1) as est_bookings,
       cast(sum(case when v.action_type = 'booking' then v.action_count else 0 end) as int) as bookings,
       round(avg(case when v.action_type = 'booking' or v.action_type = 'estimated booking' then v.total_nights_booked else null end),1) as avg_nights_per_booking,
       round(avg(case when v.action_type = 'booking' or v.action_type = 'estimated booking' then v.avg_nightly_spend else null end),2) as avg_nightly_spend,
       round(avg(v.avg_num_guests),1) as avg_num_guests,
       round(avg(v.avg_num_rooms),1) as avg_num_rooms,
       round(avg(v.avg_days_out),1) as avg_days_out,
       round(count(v.location_id)/count(distinct v.unique_id),2) as avg_properties_viewed_pp,
       count(distinct v.location_id)  as num_property_metrics,
       count(distinct v.unique_id)/count(distinct v.location_id)  as uniques_pp,
       cast(sum(case when v.action_type = 'pageviews' then v.action_count else null end) as int)/count(distinct v.location_id) as pageviews_pp,
       cast(sum(case when v.action_type = 'click' then v.action_count else null end )as int)/count(distinct v.location_id) as clicks_pp,
       round(sum(case when v.action_type = 'estimated booking' then v.action_count else null end),1)/count(distinct v.location_id) as est_bookings_pp,
       cast(sum(case when v.action_type = 'booking' then v.action_count else 0 end) as int)/count(distinct v.location_id) as bookings_pp
from display_ads.sales.vw_user_location_interactions_daily v
join brand_property_locations bpl on bpl.location_id = v.location_id
join (select distinct unique_id, max(user_country_name) as user_country_name
      from rio_sf.rust.a_unique_users auu
      where ds between '&{start_dt_m30}' and '&{end_dt}'
      group by 1) auu on v.unique_id = auu.unique_id
where v.ds between '&{start_dt}' and '&{end_dt}'
group by v.ds, bpl.brand_name, bpl.brand_code, bpl.property_country_name

union all

select v.ds,
       bpl.brand_name,
       bpl.brand_code,
       'Overall Brand'     as property_country_name,
       --'Overall Brand'     as user_country_name,
       count(distinct v.unique_id) as uniques,
       cast(sum(case when v.action_type = 'pageviews' then v.action_count else null end) as int) as pageviews,
       cast(sum(case when v.action_type = 'click' then v.action_count else null end )as int) as clicks,
       round(sum(case when v.action_type = 'estimated booking' then v.action_count else null end),1) as est_bookings,
       cast(sum(case when v.action_type = 'booking' then v.action_count else 0 end) as int) as bookings,
       round(avg(case when v.action_type = 'booking' or v.action_type = 'estimated booking' then v.total_nights_booked else null end),1) as avg_nights_per_booking,
       round(avg(case when v.action_type = 'booking' or v.action_type = 'estimated booking' then v.avg_nightly_spend else null end),2) as avg_nightly_spend,
       round(avg(v.avg_num_guests),1) as avg_num_guests,
       round(avg(v.avg_num_rooms),1) as avg_num_rooms,
       round(avg(v.avg_days_out),1) as avg_days_out,
       round(count(v.location_id)/count(distinct v.unique_id),2) as avg_properties_viewed_pp,
       count(distinct v.location_id)  as num_property_metrics,
       count(distinct v.unique_id)/count(distinct v.location_id)  as uniques_pp,
       cast(sum(case when v.action_type = 'pageviews' then v.action_count else null end) as int)/count(distinct v.location_id) as pageviews_pp,
       cast(sum(case when v.action_type = 'click' then v.action_count else null end )as int)/count(distinct v.location_id) as clicks_pp,
       round(sum(case when v.action_type = 'estimated booking' then v.action_count else null end),1)/count(distinct v.location_id) as est_bookings_pp,
       cast(sum(case when v.action_type = 'booking' then v.action_count else 0 end) as int)/count(distinct v.location_id) as bookings_pp
from display_ads.sales.vw_user_location_interactions_daily v
join brand_property_locations bpl on bpl.location_id = v.location_id
join (select distinct unique_id, max(user_country_name) as user_country_name
      from rio_sf.rust.a_unique_users auu
      where ds between '&{start_dt_m30}' and '&{end_dt}'
      group by 1) auu on v.unique_id = auu.unique_id
where v.ds between '&{start_dt}' and '&{end_dt}'
group by v.ds, bpl.brand_name, bpl.brand_code --, property_country_name, user_country_name


;

commit;
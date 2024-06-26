-----------------------------------------------------------
--  summarizes interactions by a user by advertiser by day
------------------------------------------------------------

begin;

delete from  &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily
where ds between '&{start_dt}' and '&{end_dt}';

insert into  &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily
select
    inter.ds,
    inter.unique_id,
    geo.COUNTRY                   as hotel_country,
    advertisers.dfp_advertiser_id as advertiser_id,


    --- clicks
    sum( case when action_type = 'click' then action_count else 0 end ) as click_count,
    sum( case when action_type = 'click' then 1 else 0 end ) as click_count_distinct,
    --min( case when action_type = 'click' then min_timestamp else null end ) as min_click_timestamp,
    --max( case when action_type = 'click' then max_timestamp else null end ) as max_click_timestamp,

    -- es bookings
    sum( case when action_type = 'estimated booking'
          and simple_placetype_name = 'Accomodation' then action_count else 0 end ) as hotel_estimated_bookings,
    sum( case when action_type = 'estimated booking'
          and simple_placetype_name = 'Accomodation' and action_count > 0 then 1 else 0 end ) as hotel_estimated_bookings_distinct,
    --min( case when action_type = 'estimated booking'
          --and simple_placetype_name = 'Accomodation' then min_timestamp else null end ) as min_booking_timestamp,
    --max( case when action_type = 'estimated booking'
          --and simple_placetype_name = 'Accomodation' then max_timestamp else null end ) as max_booking_timestamp,

    -- detail pageview
    sum( case when action_type = 'pageviews' then action_count else 0 end ) as pv_count,
    sum( case when action_type = 'pageviews' then 1 else 0 end ) as pv_count_distinct,
    --min( case when action_type = 'pageviews' then min_timestamp else null end ) as min_pv_timestamp,
    --max( case when action_type = 'pageviews' then max_timestamp else null end ) as max_pv_timestamp,

    -- accomodation actions
    --sum( case when action_type = 'click' and simple_placetype_name = 'Accomodation' then action_count else 0 end ) as accomodation_click_count,
    --sum( case when action_type = 'pageviews' and simple_placetype_name = 'Accomodation' then action_count else 0 end ) as accomodation_pv_count,
    --sum( case when action_type = 'click' and simple_placetype_name = 'Accomodation' then 1 else 0 end ) as num_distinct_accomodations_clicked,
    --sum( case when action_type = 'pageviews' and simple_placetype_name = 'Accomodation' then 1 else 0 end ) as num_distinct_accomodations_viewed,

    sum(case when action_type = 'estimated booking' and simple_placetype_name = 'Accomodation' and action_count > 0 then total_nights_booked else 0 end) AS total_nights_booked,
    avg(case when action_type = 'estimated booking' and simple_placetype_name = 'Accomodation' and action_count > 0 then avg_nightly_spend::float else null end) AS avg_nightly_booking_spend,
    --avg(case when action_type = 'click' and simple_placetype_name = 'Accomodation' then avg_nightly_spend::float else null end) AS avg_nightly_click_spend,
    avg(case when action_type = 'estimated booking' and simple_placetype_name = 'Accomodation' and action_count > 0 then avg_num_guests::float else null end) AS avg_num_booking_guests,
    --avg(case when action_type = 'click' and simple_placetype_name = 'Accomodation' then avg_num_guests::float else null end) AS avg_num_click_guests,
    avg(case when action_type = 'estimated booking' and simple_placetype_name = 'Accomodation' and action_count > 0 then avg_num_rooms::float else null end) AS avg_num_booking_rooms,
    --avg(case when action_type = 'click' and simple_placetype_name = 'Accomodation' then avg_num_rooms::float else null end) AS avg_num_click_rooms,
    avg(case when action_type = 'estimated booking' and simple_placetype_name = 'Accomodation' and action_count > 0 then avg_days_out else null end) AS avg_accomodation_days_out,
    round((sum(case when action_type = 'estimated booking' and action_count > 0 then estimated_gbv else null end) /
            nullif(sum(case when action_type = 'estimated booking' and action_count > 0 then action_count else null end),0))
            / nullif(avg(case when action_type = 'estimated booking' and action_count > 0 then total_nights_booked else null end),0),2)   as avg_daily_rate

from  display_ads.sales.vw_user_location_interactions_daily inter
      left join (select distinct location_id, COUNTRY from rio_sf.hotels_sst.a_location_details_latest) geo on inter.location_id = geo.location_id
      join &{pipeline_schema_sf}.hotel_advertiser_location_mapping advertisers on inter.location_id = advertisers.location_id
where inter.ds between '&{start_dt}' and '&{end_dt}' and simple_placetype_name = 'Accomodation'
group by ds, unique_id, advertiser_id, hotel_country

;

commit;

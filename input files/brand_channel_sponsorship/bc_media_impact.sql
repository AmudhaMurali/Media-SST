begin;
delete from &{pipeline_schema_sf}.bc_media_impact
where ds BETWEEN '&{start_dt}' AND '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_media_impact

with brand_channel_exposed_uniques as (
select distinct ds,ta_unique_id as exposed_unique_id
from RIO_SF.DISPLAY_SALES.SPOCK_SPONSORSHIP_CAMPAIGNS a
join USER_TRACKING.PUBLIC.BLESSED_PAGEVIEWS b
on a.locale = b.locale
and geo_id = geo_location_id
and ds >= campaign_start_date and ds <= campaign_end_date
where campaign_type = 'Brand Channel' --and campaign_type = 'Destinations'
and  ds BETWEEN  '&{start_dt_m30}' AND '&{end_dt}'
and page_name = 'Tourism'),

unexposed_uniques as
(select distinct a.ds,a.ta_unique_id as unexposed_unique_id
from "USER_TRACKING"."PUBLIC"."BLESSED_PAGEVIEWS" a
left join brand_channel_exposed_uniques b on a.ta_unique_id = b.exposed_unique_id
left join DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS s on a.locale = s.locale and geo_id = geo_location_id
where b.exposed_unique_id is null
and  a.ds BETWEEN  '&{start_dt_m30}' AND '&{end_dt}'
and page_name = 'Tourism' and campaign_type != 'Destinations'),

impact as (

    select ds,
            unique_id,
            sum(case when action_type = 'pageviews' then action_count end) as total_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Attraction' then action_count end) as attraction_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Accomodation' then action_count end) as accomodation_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Eatery' then action_count end) as restaurant_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name in ('Accomodation', 'Eatery', 'Attraction') then action_count end) as poi_pageviews,
            sum(case when action_type = 'estimated booking' then action_count end) as estimated_bookings,
            sum(case when action_type = 'click' then action_count end) as clicks

    from display_ads.sales.vw_user_location_interactions_daily
    where ds BETWEEN '&{start_dt}' AND '&{end_dt}'
     group by 1,2
),

bc_unique_funnel as (
select pv.ds,
       pv.ta_unique_id,
       max(marketing_campaign_id) as marketing_campaign_id,
        max(client_type) as client_type,
       max(user_country_name) as user_country_name,
       max(locale) as locale,
        max(total_pageviews) as total_pageviews_by_users,
        max(attraction_pageviews) as total_attraction_pageviews_by_users,
        max(accomodation_pageviews) as total_accomodation_pageviews_by_users,
        max(poi_pageviews) as total_poi_pageviews_by_users,
        max(clicks) as total_clicks_by_users,
        max(estimated_bookings) as total_estimated_bookings_by_users,
        max(case when exposed_unique_id is not null then 1
            when unexposed_unique_id is not null then 0
            end) as saw_brand_channel_page,
       max(case when pv.geo_location_id = bch.location_id and bc_page_name in ('brand_channel_home') then pv.geo_location_id end) as bc_geo_id,
       --max(case when pv.geo_location_id = bch.location_id and bc_page_name in ('brand_channel_home') then bch.sponsor_name end) as bc_geo_name,
       count(distinct pv.ta_unique_id) as uniques
from   ( SELECT *, ROW_NUMBER() OVER (PARTITION BY ds, ta_unique_id ORDER BY event_timestamp) user_journey_page_view_order_day --table has page_view_order by client session, this will roll the order to the day
             FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
              WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
        ) pv
LEFT JOIN DISPLAY_ADS.SALES.all_geo_locations gloc on pv.detail_location_id = gloc.location_id
LEFT JOIN impact i on i.ds = pv.ds and i.unique_id = pv.ta_unique_id
LEFT JOIN (SELECT DISTINCT location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bch ON bch.location_id = pv.geo_location_id
left join brand_channel_exposed_uniques bu on bu.exposed_unique_id = pv.ta_unique_id and bu.ds = pv.ds
left join unexposed_uniques uu on uu.unexposed_unique_id = pv.ta_unique_id and uu.ds = pv.ds
where pv.ds BETWEEN '&{start_dt}' AND '&{end_dt}' and pv.is_blessed = true --and (exposed_unique_id is not null or unexposed_unique_id is not null)
group by 1,2

)


select ds,
       coalesce(saw_brand_channel_page, FALSE) as saw_brand_channel_page,
       bc_geo_id,
       sponsor_name as bc_geo_name,
       client_type,
       user_country_name,
       locale,
       marketing_campaign_id,
       sum(uniques) as uniques,
       sum(total_pageviews_by_users) as total_pageviews_by_users,
       sum(total_attraction_pageviews_by_users) as total_attraction_pageviews_by_users,
       sum(total_accomodation_pageviews_by_users) as total_accomodation_pageviews_by_users,
       sum(total_poi_pageviews_by_users) as total_poi_pageviews_by_users,
       sum(total_clicks_by_users) as total_clicks_by_users,
       sum(total_estimated_bookings_by_users) as total_estimated_bookings_by_users
from bc_unique_funnel f
left join (SELECT DISTINCT location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bch ON bch.location_id = f.bc_geo_id
group by 1,2,3,4,5,6,7,8

;

commit;
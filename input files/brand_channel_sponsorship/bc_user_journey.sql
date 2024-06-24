begin;
delete from &{pipeline_schema_sf}.bc_user_journey
where ds BETWEEN '&{start_dt}' AND '&{end_dt}';


insert into &{pipeline_schema_sf}.bc_user_journey


with bc_page_order as (
    select ds, ta_unique_id, geo_location_id, max(saw_brand_channel_page) saw_brand_channel_page, max(saw_bc_page_order) as saw_bc_page_order
from (
         SELECT ta_unique_id,
                ds,
                bc_page_name,
                page_name,
                user_journey_page_view_order_day,
                case when  bc_page_name = 'brand_channel_home' then geo_location_id end as geo_location_id,
                MAX(CASE WHEN bc_page_name = 'brand_channel_home' THEN TRUE ELSE FALSE END) OVER (PARTITION BY ds, ta_unique_id) AS saw_brand_channel_page,
                min(case when  bc_page_name = 'brand_channel_home' then user_journey_page_view_order_day end) OVER (PARTITION BY ds, ta_unique_id) as saw_bc_page_order
                     FROM
                          ( SELECT ta_unique_id, ds, bc_page_name, page_name, geo_location_id, event_timestamp,
                                  ROW_NUMBER() OVER (PARTITION BY ds, ta_unique_id ORDER BY event_timestamp) user_journey_page_view_order_day --table has page_view_order by client session, this will roll the order to the day
                            FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
                            WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                            GROUP BY 1,2,3,4,5,6
                  )
            GROUP BY 1, 2, 3, 4, 5, 6
     )
    where saw_bc_page_order is not null
    group by 1,2,3
),

impact as (

    select ds,
            unique_id,
            sum(case when action_type = 'pageviews' then action_count end) as total_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Attraction' then action_count end) as attraction_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Accomodation' then action_count end) as accomodation_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name = 'Eatery' then action_count end) as restaurant_pageviews,
            sum(case when action_type = 'pageviews' and simple_placetype_name in ('Accomodation', 'Eatery', 'Attraction') then action_count end) as poi_pageviews,
            sum(case when action_type in ('estimated booking') then action_count end) as estimated_bookings

    from display_ads.sales.vw_user_location_interactions_daily
    where ds BETWEEN '&{start_dt}' AND '&{end_dt}'
     group by 1,2



),

bc_interactions as (

         SELECT pv.ds,
                ta_unique_id,
                max(COALESCE(impressions, 0))  AS impressions,
                max(COALESCE(interactions, 0)) AS interactions,
                Count(DISTINCT page_uid)       AS bc_home_page_views
         FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily pv
                  LEFT JOIN (SELECT ds,
                                    unique_id,
                                    Sum(impressions)  AS impressions,
                                    Sum(interactions) AS interactions
                             FROM DISPLAY_ADS.sales.bc_unique
                             WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                             GROUP BY 1, 2) bcu ON bcu.ds = pv.ds AND bcu.unique_id = pv.ta_unique_id
         WHERE pv.ds BETWEEN '&{start_dt}' AND '&{end_dt}'
           AND pv.is_blessed = TRUE
           and pv.bc_page_name = 'brand_channel_home'
         GROUP BY 1, 2

),

pageviews_with_display_ad_as_last_referer as (
    select ds, page_uid, ta_unique_id, client_session_id, event_timestamp
    from display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
    where ds BETWEEN '&{start_dt}' AND '&{end_dt}'
    and (last_referrer like '%google.ads%' or last_referrer like '%track.celtra%' or last_referrer like '%about:blank%') and bc_page_name = 'brand_channel_home'
    and is_blessed = true
),

min_timestamp as (
    select ds, client_session_id, min(event_timestamp) event_timestamp
    from display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
    where ds BETWEEN '&{start_dt}' AND '&{end_dt}' and client_session_id in (select distinct client_session_id from pageviews_with_display_ad_as_last_referer)
    group by 1,2
),

internal_external_ad as (
    select p.ds, p.ta_unique_id, max(iff(p.event_timestamp > m.event_timestamp, 'Internal_Ad', 'External_Ad')) as bc_entry_point
    from pageviews_with_display_ad_as_last_referer p
    join min_timestamp m
    on p.client_session_id = m.client_session_id and p.ds = m.ds
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
        max(estimated_bookings) as total_estimated_bookings_by_users,
        max(bc.saw_brand_channel_page) as saw_brand_channel_page,
       max(case when pv.geo_location_id = bch.location_id and bc_page_name in ('brand_channel_home') then pv.geo_location_id end) as bc_geo_id,
       max(case when pv.geo_location_id = bch.location_id and bc_page_name in ('brand_channel_home') then bch.sponsor_name end) as bc_geo_name,
       max((case when bc_page_name in ('brand_channel_home') and user_journey_page_view_order_day = bc.saw_bc_page_order then COALESCE(iea.bc_entry_point, referrer_type_detail) end)) as bc_entry_point,
       count(distinct pv.ta_unique_id) as uniques,
       max(bci.interactions) as bc_interactions,
       max(bci.impressions) as bc_shelf_impressions,
       max(bci.bc_home_page_views) as bc_page_views,
       count(distinct case when bc_page_name = 'brand_channel_poi' and user_journey_page_view_order_day > bc.saw_bc_page_order then pv.ta_unique_id end) as moved_to_view_poi_uniques,
       count(distinct case when bc_page_name = 'brand_channel_poi' and user_journey_page_view_order_day > bc.saw_bc_page_order and gloc.location_placetype_name = 'Accomodation' then pv.ta_unique_id end) as moved_to_view_hotel_poi_uniques,
       count(distinct case when bc_page_name = 'brand_channel_poi' and user_journey_page_view_order_day > bc.saw_bc_page_order and gloc.location_placetype_name = 'Attraction' then pv.ta_unique_id end) as moved_to_view_attraction_poi_uniques,
       count(distinct case when bc_page_name = 'brand_channel_poi' and user_journey_page_view_order_day > bc.saw_bc_page_order and gloc.location_placetype_name = 'Eatery' then pv.ta_unique_id end) as moved_to_view_eatery_poi_uniques,
       count(distinct case when bc_page_name = 'brand_channel_home' and user_journey_page_view_order_day > bc.saw_bc_page_order then pv.ta_unique_id end) as moved_back_to_bc_page_uniques,
       count(distinct case when bc_page_name in ('brand_channel_trip') and user_journey_page_view_order_day > bc.saw_bc_page_order then pv.ta_unique_id  end) as moved_to_view_trips_uniques,
       count(distinct case when bc_page_name in ('brand_channel_article') and user_journey_page_view_order_day > bc.saw_bc_page_order then pv.ta_unique_id  end) as moved_to_view_articles_uniques,
       count(distinct case when bc_page_name in ('brand_channel_home') and referrer_type = 'ta-referral'  and referrer_type_detail = 'tripadvisor_other' then pv.ta_unique_id end) as entered_bc_via_listing_page_uniques,
       count(distinct case when bc_page_name in ('brand_channel_home') and referrer_type_detail = 'tripadvisor_search' then pv.ta_unique_id  end) as entered_bc_via_search_uniques,
       count(distinct case when bc_page_name in ('brand_channel_home') and referrer_type_detail in ('non-ta-referral', 'non-ta-other') then pv.ta_unique_id  end) as entered_bc_outside_of_trip_uniques
from   ( SELECT ds, ta_unique_id, event_timestamp, detail_location_id, geo_location_id, marketing_campaign_id, client_type, user_country_name, locale, bc_page_name, referrer_type_detail,referrer_type, is_blessed,
                ROW_NUMBER() OVER (PARTITION BY ds, ta_unique_id ORDER BY event_timestamp) user_journey_page_view_order_day --table has page_view_order by client session, this will roll the order to the day
             FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
              WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
              GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
        ) pv
LEFT JOIN DISPLAY_ADS.SALES.all_geo_locations gloc on pv.detail_location_id = gloc.location_id
LEFT JOIN impact i on i.ds = pv.ds and i.unique_id = pv.ta_unique_id
LEFT JOIN (SELECT DISTINCT location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bch ON bch.location_id = pv.geo_location_id
LEFT JOIN bc_page_order bc on bc.ta_unique_id = pv.ta_unique_id and bc.ds = pv.ds and bc.geo_location_id = pv.geo_location_id
LEFT JOIN bc_interactions bci on bci.ta_unique_id = pv.ta_unique_id and bci.ds = pv.ds
LEFT JOIN internal_external_ad iea on iea.ta_unique_id = pv.ta_unique_id and iea.ds = pv.ds
where pv.ds BETWEEN '&{start_dt}' AND '&{end_dt}' and pv.is_blessed = true
group by 1,2

)

select ds,
       coalesce(saw_brand_channel_page, FALSE) as saw_brand_channel_page,
       bc_entry_point,
       bc_geo_id,
       bc_geo_name,
       client_type,
       user_country_name,
       locale,
       marketing_campaign_id,
       sum(uniques) as uniques,
       sum(bc_interactions) as bc_interactions,
       sum(bc_shelf_impressions) as bc_shelf_impressions,
       sum(bc_page_views) as bc_page_views,
       sum(moved_to_view_poi_uniques) as moved_to_view_poi_uniques,
       sum(moved_to_view_hotel_poi_uniques) as moved_to_view_hotel_poi_uniques,
       sum(moved_to_view_attraction_poi_uniques) as moved_to_view_attraction_poi_uniques,
       sum(moved_to_view_eatery_poi_uniques) as moved_to_view_eatery_poi_uniques,
       sum(moved_back_to_bc_page_uniques) as moved_back_to_bc_page_uniques,
       sum(moved_to_view_trips_uniques) as moved_to_view_trips_uniques,
       sum(moved_to_view_articles_uniques) as moved_to_view_articles_uniques,
       sum(entered_bc_via_listing_page_uniques) as entered_bc_via_listing_page_uniques,
       sum(entered_bc_via_search_uniques) as entered_bc_via_search_uniques,
       sum(entered_bc_outside_of_trip_uniques) as entered_bc_outside_of_trip_uniques,
       sum(total_pageviews_by_users) as total_pageviews_by_users,
       sum(total_attraction_pageviews_by_users) as total_attraction_pageviews_by_users,
       sum(total_accomodation_pageviews_by_users) as total_accomodation_pageviews_by_users,
       sum(total_poi_pageviews_by_users) as total_poi_pageviews_by_users,
       sum(total_estimated_bookings_by_users) as total_estimated_bookings_by_users
from bc_unique_funnel
group by 1,2,3,4,5,6,7,8,9






;

commit;


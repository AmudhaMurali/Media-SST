begin;

delete from &{pipeline_schema_sf}.bc_agg
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema_sf}.bc_agg

SELECT ds,
       element_type,
       bc_geo_id,
       bc_geo_name,
       op1_order_id,
       user_country_id,
       user_country_name,
       marketing_campaign_id,
       os_type,
       locale,
       video_completion_rate,
       Count(DISTINCT ta_unique_id) AS uniques,
       Sum(bc_home_page_views) AS bc_home_page_views,
       Sum(impressions) AS impressions,
       Sum(interactions) AS interactions,
       Count(DISTINCT CASE WHEN interactions > 0 THEN ta_unique_id ELSE NULL END) AS uniques_w_interactions,
       sum(attraction_pageviews) as attraction_pageviews,
       sum(accomodation_pageviews) as accomodation_pageviews,
       sum(restaurant_pageviews) as restaurant_pageviews,
       sum(total_pageviews) as total_pageviews,
       sum(poi_pageviews) as poi_pageviews,
       sum(estimated_bookings) as estimated_bookings,
       max(bc_entry_point) as bc_entry_point

FROM (
         SELECT pv.ds,
                'Unique Totals' AS element_type,
                pv.geo_location_id AS bc_geo_id,
                bc.sponsor_name AS bc_geo_name,
                ta_unique_id,
                pv.os_type_id AS  os_type,
                pv.locale,
                video_completion_rate,
                uu.attraction_pageviews as attraction_pageviews,
                uu.accomodation_pageviews as accomodation_pageviews,
                uu.restaurant_pageviews as restaurant_pageviews,
                uu.total_pageviews as total_pageviews,
                uu.poi_pageviews as poi_pageviews,
                uu.estimated_bookings as estimated_bookings,
                max(bcu.op1_order_id) as op1_order_id,
                max(COALESCE(impressions, 0)) AS impressions,
                max(COALESCE(interactions, 0)) AS interactions,
                max(case when pv.bc_page_name in ('brand_channel_home') then pv.referrer_type_detail end) as bc_entry_point,
                Max(pv.marketing_campaign_id) AS marketing_campaign_id,
                Max(pv.user_country_id) AS user_country_id,
                Max(pv.user_country_name) AS user_country_name,
                Count(DISTINCT page_uid) AS bc_home_page_views
         FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily pv
                   LEFT JOIN (SELECT ds,
                               unique_id,
                               op1_order_id,
                               bc_geo_id,
                               bc_geo_name,
                               max(video_completion_rate) as video_completion_rate,
                               Sum(impressions)  AS impressions,
                               Sum(interactions) AS interactions
                        FROM &{pipeline_schema_sf}.bc_unique
                        WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                        GROUP BY 1, 2, 3, 4, 5) bcu ON bcu.ds = pv.ds AND bcu.unique_id = pv.ta_unique_id AND bcu.bc_geo_id = pv.geo_location_id
                    LEFT JOIN (SELECT DISTINCT location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels
                                                                    ) bc ON bc.location_id = pv.geo_location_id
                    LEFT JOIN (select ds,
                                   unique_id,
                                   sum(case when action_type = 'pageviews' then action_count end) as total_pageviews,
                                   sum(case when action_type = 'pageviews' and simple_placetype_name = 'Attraction' then action_count end) as attraction_pageviews,
                                   sum(case when action_type = 'pageviews' and simple_placetype_name = 'Accomodation' then action_count end) as accomodation_pageviews,
                                   sum(case when action_type = 'pageviews' and simple_placetype_name = 'Eatery' then action_count end) as restaurant_pageviews,
                                   sum(case when action_type = 'pageviews' and simple_placetype_name in ('Accomodation', 'Eatery', 'Attraction') then action_count end) as poi_pageviews,
                                   sum(case when action_type in ('estimated booking') then action_count end) as estimated_bookings
                            from display_ads.sales.vw_user_location_interactions_daily
                            where ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                            group by 1,2) uu on uu.ds = pv.ds and uu.unique_id = pv.ta_unique_id
                            WHERE pv.ds BETWEEN '&{start_dt}' AND '&{end_dt}'

                              AND pv.is_blessed = TRUE and pv.bc_page_name = 'brand_channel_home' --limit to only the BC HOME PAGE

          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
     ) a

GROUP BY ds,
       element_type,
       bc_geo_id,
       bc_geo_name,
       op1_order_id,
       user_country_id,
       user_country_name,
       marketing_campaign_id,
       os_type,
       locale,
       video_completion_rate

UNION ALL


         SELECT bcu.ds,
                element_type,
                bc_geo_id,
                bc_geo_name,
                op1_order_id,
                pv.user_country_id,
                pv.user_country_name,
                bcu.marketing_campaign_id,
                bcu.os_type,
                pv.locale,
                video_completion_rate,
                Count(DISTINCT bcu.unique_id)                                                  AS uniques,
                NULL                                                                       AS bc_home_page_views,
                Sum(impressions)                                                           AS impressions,
                Sum(interactions)                                                          AS interactions,
                Count(DISTINCT CASE WHEN (interactions) > 0 THEN bcu.unique_id ELSE NULL END) AS uniques_w_interactions,
                null as total_pageviews,
                null as attraction_pageviews,
                null as accomodation_pageviews,
                null as restaurant_pageviews,
                null as poi_pageviews,
                null as estimated_bookings,
                rtd.referrer_type_detail as bc_entry_point
         FROM  &{pipeline_schema_sf}.bc_unique bcu
                  LEFT JOIN (SELECT distinct ds, ta_unique_id,
                                            max(locale) as locale,
                                            Max(user_country_id)   user_country_id,
                                            Max(user_country_name) user_country_name
                             FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
                             WHERE ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                             GROUP BY 1, 2) pv ON pv.ta_unique_id = bcu.unique_id and pv.ds = bcu.ds
                  LEFT JOIN rio_sf.rust.a_unique_users auu on auu.ds = bcu.ds and auu.unique_id = bcu.unique_id
                  LEFT JOIN (
                                 SELECT ds,
                                 ta_unique_id,
                                 referrer_type_detail
                                    FROM (SELECT ds,
                                                ta_unique_id,
                                                referrer_type_detail,
                                                ROW_NUMBER() OVER (PARTITION BY ds, ta_unique_id ORDER BY event_timestamp) user_journey_page_view_order_day
                                         FROM display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
                                         WHERE bc_page_name = 'brand_channel_home'
                                           AND ds BETWEEN '&{start_dt}' AND '&{end_dt}')
                                    WHERE user_journey_page_view_order_day = 1
                                 ) rtd on rtd.ds = bcu.ds and bcu.unique_id = rtd.ta_unique_id -- gets the referrer type for how the user FIRST saw the brand channel home page
         WHERE bcu.ds BETWEEN '&{start_dt}' AND '&{end_dt}' and auu.is_blessed = TRUE
         AND bcu.unique_id in (select distinct ta_unique_id
                                    from display_ads.BRAND_CHANNEL_USER_JOURNEY.aggregate_page_views_daily
                                    where ds BETWEEN '&{start_dt}' AND '&{end_dt}' AND is_blessed = TRUE and bc_page_name = 'brand_channel_home') --ensures we only look at impressions and interactions of users who had a registered BC Page View
         GROUP BY bcu.ds,
                  element_type,
                  bc_geo_id,
                  bc_geo_name,
                  op1_order_id,
                  pv.user_country_id,
                  pv.user_country_name,
                  bcu.marketing_campaign_id,
                  bcu.os_type,
                  pv.locale,
                  video_completion_rate,
                  rtd.referrer_type_detail

;

commit;

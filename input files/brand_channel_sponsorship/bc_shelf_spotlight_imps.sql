-- daily impressions/interactions on brand channel pages - pulled from t_mixer_impressions_new

begin;

delete from &{pipeline_schema_sf}.bc_shelf_spotlight_imps
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_shelf_spotlight_imps

select
    ui.ds,
    ui.unique_id,
    au.os_type,
    mi.locale,
    au.marketing_campaign_id,
    au.commerce_country_id as user_country_id,
    au.user_country_name,
    mi.geo_scope as bc_geo_id,
    bc.sponsor_name as bc_geo_name,
    trim(ui.ui_element_keys:feedSectionId,'"') as feed_section_id,
    mi.puid,
    mi.cluster_id,
    mi.placement,
    UPPER(CASE
              WHEN CONTAINS(lower(title_text), 'brand_channel_hero_card_') then 'hero content card#1'
              WHEN CONTAINS(lower(title_text), 'brand_channel_hero_card2_') then 'hero content card#2'
              WHEN CONTAINS(lower(title_text), 'brand_channel_hero_card3_') then 'hero content card#3'
              WHEN (CONTAINS(lower(shelf_title_key), 'brand_channel_discovery_carousel_')
                or lower(shelf_title_key) = 'brand_channel_discovery_carousel')  then 'discovery_carousel#1'
              WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_discovery_carousel2_')  then 'discovery_carousel#2'
              WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_discovery_carousel3_')  then 'discovery_carousel#3'
              WHEN lower(shelf_title_key) = 'brand_channel_reviews' then 'tripadvisor_reviews'
              WHEN lower(list_type) = 'certificate_of_excellence' or lower(list_type) = 'loved_for' or lower(shelf_title_key) like 'brand_channel_top_rated_pois_%' then 'top_rated_pois'
              WHEN (CONTAINS(lower(shelf_title_key), 'brand_channel_central_nav_')
                or lower(shelf_title_key) = 'brand_channel_central_nav')  then 'central_nav#1'
              WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_central_nav2_')  then 'central_nav#2'
              WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_central_nav3_')  then 'central_nav#3'
              WHEN (CONTAINS(lower(title_text), 'brand_channel_map_')
                or lower(title_text) in ('brand_channel_Visa_Athens_map_shelf_brand_channel', 'destination_driver_cta')) then 'map#1'
              WHEN CONTAINS(lower(title_text), 'brand_channel_map2_') then 'map#2'
              WHEN CONTAINS(lower(title_text), 'brand_channel_map3_') then 'map#3'
              WHEN (CONTAINS(lower(title_text), 'brand_channel_forums_shelf_')
                or lower(title_text) = 'brand_channel_forums_shelf')  then 'forums#1'
              WHEN CONTAINS(lower(title_text), 'brand_channel_forums_shelf2_')  then 'forums#2'
              WHEN CONTAINS(lower(title_text), 'brand_channel_forums_shelf3_')  then 'forums#3'
              WHEN lower(shelf_title_key) = 'brand_channel_booking_module' then 'booking_module'
              WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_custom')  then 'custom_shelf'
              WHEN (CONTAINS(lower(shelf_title_key), 'brand_channel_reviews_')
                or lower(shelf_title_key) = 'brand_channel_reviews')   then 'review_spotlight#1'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_reviews2_')  then 'review_spotlight#2'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_reviews3_')  then 'review_spotlight#3'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_traveler_spotlight_') then 'traveler_spotlight#1'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_traveler_spotlight2_') then 'traveler_spotlight#2'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_traveler_spotlight3_') then 'traveler_spotlight#3'
             WHEN (CONTAINS(lower(title_text), 'brand_channel_content_spotlight_')
                or lower(title_text) = 'discover_geo')  then 'content_spotlight#1'
             WHEN CONTAINS(lower(title_text), 'brand_channel_content_spotlight2_')  then 'content_spotlight#2'
             WHEN CONTAINS(lower(title_text), 'brand_channel_content_spotlight3_')  then 'content_spotlight#3'
             WHEN lower(list_type) = 'top_placetype'  then 'destination_essentials'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_poi_shelf_') then 'poi_shelf#1'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_poi_shelf2_') then 'poi_shelf#2'
             WHEN CONTAINS(lower(shelf_title_key), 'brand_channel_poi_shelf3_') then 'poi_shelf#3'
    else 'MAPPING_NOT_DEFINED' END)   as bc_shelf, -- mapping here: https://docs.tamg.io/display/TAPS/Brand+Channel+Analytics+Mapping#
    mi.item_category,
    mi.curated_shelf_type,
    mi.shelf_title_key,
    mi.list_type,
    mi.position,
    mi.item_list_id
from USER_TRACKING.PUBLIC.F_USER_INTERACTION_DEPRECATED ui
join rio_sf.rust.a_unique_users au on ui.unique_id = au.unique_id and ui.ds = au.ds
                                   and au.ds between '&{start_dt}' and '&{end_dt}'
join rio_sf.cx_analytics.t_mixer_impressions_new mi on ui.unique_id = mi.unique_id
                                                    and ui.ds = mi.ds and trim(ui.ui_element_keys:feedSectionId,'"') = mi.feed_section_id
                                                    and mi.ds between '&{start_dt}' and '&{end_dt}'
join (select distinct location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bc on bc.location_id = mi.geo_scope -- only brand channels
where ui.ds between '&{start_dt}' and '&{end_dt}'
and mi.placement in ('HOME_REDESIGN','NEARBY_REDESIGN') -- filter if you want to only include tourism pages
and ui.ui_element_source = 'Mixer' -- mixer feed
and ui.ui_element_type = 'feedScroll' -- determines if the impression was in the user's view

;

commit;

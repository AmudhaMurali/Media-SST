
-----------------------------------------------------------
-- remake of the Woodsy Job visa_trips_poi_actions_v3
-- aggregates the clicks, likes, see_more, shares and map
-- clicks for each detail id in a sponsored trip
-- this is a supporting table
-----------------------------------------------------------


begin;
delete from &{pipeline_schema}.sponsored_trips_poi_actions
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_poi_actions
select w.ds                                                                                                                    as ds,
       w.unique_id                                                                                                             as unique_id,
       uu.os_type                                                                                                              as os_type,
       uu.locale                                                                                                               as locale,
       trips.user_id                                                                                                           as user_id,
       trips.username                                                                                                          as username,
       trips.trip_title                                                                                                        as trip_title,
       trips.trip_desc                                                                                                         as trip_desc,
       cast(trim(w.ui_element_keys:tripId, '"') as int)                                                                        as tripid,
       trim(w.ui_element_keys:saveType, '"')                                                                                   as saveType,
       cast(trim(w.ui_element_keys:detailId, '"') as int)                                                                      as detailid,
       loc.primaryname                                                                                                         as detailid_name,
       sum(case when w.ui_element_keys:element in ('card_media', 'card_title','location_information') then 1 else 0 end)       as clicks,
       sum(case when w.ui_element_keys:element = 'item_like' then 1 else 0 end)                                                as likes,
       sum(case when w.ui_element_keys:element in ('item_note_click', 'expand', 'expandNote') then 1 else 0 end)               as see_more,
       sum(case when w.ui_element_keys:element = 'shareItem' then 1 else 0 end)                                                as shares,
       sum(case when w.ui_element_keys:element in ('mapPin', 'mapCard', 'mapCarousel', 'map') then 1 else 0 end)               as maps,
       uu.USER_COUNTRY_NAME                                                                                                    as USER_COUNTRY_NAME,
       uu.marketing_campaign_id                                                                                                as marketing_campaign_id
from USER_TRACKING.PUBLIC.F_USER_INTERACTION_DEPRECATED w
join (select unique_id, max(LOCALE) as LOCALE, max(os_type) as os_type, max(USER_IP_COUNTRY_NAME) as USER_COUNTRY_NAME, max(marketing_campaign_id) as marketing_campaign_id
      from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds  between '&{start_dt}' and '&{end_dt}'  AND IS_BLESSED = 1
          group by 1 ) uu on uu.unique_id = w.unique_id

JOIN &{pipeline_schema}.active_sponsored_trips_detail trips on cast(trim(w.ui_element_keys:tripId, '"') as int)  = trips.trip_id

LEFT JOIN rio_sf.public.t_location loc on loc.id = trim(w.ui_element_keys:detailId, '"')
where w.ds between '&{start_dt}' and '&{end_dt}'
and cast(trim(w.ui_element_keys:tripId, '"') as int) is not null
and cast(trim(w.ui_element_keys:tripId, '"') as int) <> '0'
and cast(trim(w.ui_element_keys:detailId, '"') as int) is not null
group by w.ds,
         w.unique_id,
         uu.os_type,
         uu.locale,
         trips.user_id,
         trips.username,
         trips.trip_title,
         trips.trip_desc,
         cast(trim(w.ui_element_keys:tripId, '"') as int), -- tripid
         trim(w.ui_element_keys:saveType, '"'),            -- savetype
         cast(trim(w.ui_element_keys:detailId, '"') as int),
         loc.primaryname,
         uu.USER_COUNTRY_NAME,
         uu.marketing_campaign_id
;

commit;
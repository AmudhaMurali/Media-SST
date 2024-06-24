
begin;
delete from &{pipeline_schema}.sponsored_trips_poi_actions_new
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_poi_actions_new
select w.ds                                                                                                                    as ds,
       w.unique_id                                                                                                             as unique_id,
       uu.os_type                                                                                                              as os_type,
       uu.locale                                                                                                               as locale,
       uu.USER_COUNTRY_NAME                                                                                                    as USER_COUNTRY_NAME,
       uu.marketing_campaign_id                                                                                                as marketing_campaign_id,
       trips.user_id                                                                                                           as user_id,
       trips.username                                                                                                          as username,
       trips.trip_title                                                                                                        as trip_title,
       trips.trip_desc                                                                                                         as trip_desc,
       COALESCE(cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int),save.trip_id)                                                  as tripid,
       save.save_type                                                                                                          as saveType,
       COALESCE(w.location_id,cast(trim(CUSTOM_DATA_JSON:locationId, '"') as int),a.location_id)                               as detailid,
       loc.primaryname                                                                                                         as detailid_name,
       sum(case when w.ITEM_TYPE = 'TripContent' and w.ITEM_NAME = 'PublicTripMapItemClick' then 1 else 0 end)   as poi_map_click,
       sum(case when w.ITEM_TYPE = 'TripContent' and w.ITEM_NAME = 'PublicTripItemCarouselScroll' then 1 else 0 end)   as poi_carousel,
       sum(case when w.ITEM_TYPE = 'TripContent' and w.ITEM_NAME = 'PublicTripItemReadMore' then 1 else 0 end)    as poi_readmore,
       sum(case when w.ITEM_TYPE = 'TripContent' and w.ITEM_NAME = 'PublicTripItemCTA' then 1 else 0 end)    as poi_clickthr,
       sum(case when w.ITEM_TYPE = 'TripContent' and w.ITEM_NAME = 'PublicTripMapItemCTA ' then 1 else 0 end)    as poi_map_clickthr,
       sum(case when w.ITEM_TYPE = 'TripsEntry' and w.ITEM_NAME = 'ItemHeart' then 1 else 0 end)    as poi_save
from USER_TRACKING.public.USER_INTERACTIONS w
left join ENTERPRISE_DATA.TRIPS.VW_SAVES_TRIPS a on a.SAVE_ID = cast(trim(CUSTOM_DATA_JSON:itemId, '"') as int)
left join ENTERPRISE_DATA.TRIPS.VW_SAVES_TRIPS save on save.location_id = w.location_id  and save.ds = w.ds and save.unique_id = w.unique_id
join (select unique_id, max(LOCALE) as LOCALE, max(os_type) as os_type, max(USER_IP_COUNTRY_NAME) as USER_COUNTRY_NAME, max(marketing_campaign_id) as marketing_campaign_id
      from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds  between '&{start_dt}' and '&{end_dt}'  AND IS_BLESSED = 1
          group by 1 ) uu on uu.unique_id = w.unique_id
JOIN &{pipeline_schema}.active_sponsored_trips_detail trips on COALESCE(cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int),save.trip_id)  = trips.trip_id
LEFT JOIN rio_sf.public.t_location loc on loc.id = COALESCE(w.location_id,cast(trim(CUSTOM_DATA_JSON:locationId, '"') as int),a.location_id)
where w.ds between '&{start_dt}' and '&{end_dt}'
and COALESCE(cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int),save.trip_id)  is not null
and COALESCE(cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int),save.trip_id)  <> '0'
and COALESCE(w.location_id,cast(trim(CUSTOM_DATA_JSON:locationId, '"') as int),a.location_id) is not null
group by all

;

commit;
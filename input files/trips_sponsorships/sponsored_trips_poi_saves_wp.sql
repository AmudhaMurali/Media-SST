
-----------------------------------------------------------
-- remake of the Woodsy Job sponsored_trips_poi_saves_
-- aggregates the saves clicks for each detail id in
-- a sponsored trip
-- this is a supporting table
-----------------------------------------------------------


begin;
delete from &{pipeline_schema}.sponsored_trips_poi_saves_wp
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_poi_saves_wp
SELECT uu.ds                as ds,
       uu.UNIQUE_ID         as unique_id,
       uu.OS_TYPE           as os_type,
       uu.LOCALE            as locale,
       a.USER_ID            as user_id,
       trips.username       as username,
       a.LIST_ID            as tripid,
       trips.trip_title     as trip_title,
       a.SAVES_TYPE_ID      as detailid,
       loc.primaryname      as detailid_name,
       1                    as saves,
       USER_COUNTRY_NAME    as USER_COUNTRY_NAME,
       marketing_campaign_id as marketing_campaign_id

FROM

 (select unique_id, ds,max(LOCALE) as LOCALE, max(os_type) as os_type, max(user_id) as user_id ,max(USER_IP_COUNTRY_NAME) as USER_COUNTRY_NAME, max(marketing_campaign_id) as marketing_campaign_id
         from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds  between '&{start_dt}' and '&{end_dt}'  AND IS_BLESSED = 1 and user_id is not null
          group by 1,2 ) uu

LEFT JOIN (SELECT si.id,
                  si.LIST_ID,
                  l.USER_ID,
                  l.SAVE_TYPE as SAVES_TYPE,
                  l.SAVE_ID as SAVES_TYPE_ID,
                  to_date(si.CREATED) as created,
                  m.user_id as muser_id
           FROM rio_sf.cx_analytics.t_saves_items si
             LEFT JOIN rio_sf.cx_analytics.t_saves_all_archive l ON l.id = si.save_id
           LEFT JOIN rio_sf.cx_analytics.member_metadata m ON l.user_id = m.memberid
           WHERE to_date(si.CREATED) between '&{start_dt}' and '&{end_dt}') a on uu.ds = a.created
                                                        and uu.USER_ID = a.muser_id
LEFT JOIN rio_sf.public.t_location loc on loc.id = 	a.SAVES_TYPE_ID and a.SAVES_TYPE IN (5, 13)
JOIN &{pipeline_schema}.active_sponsored_trips_detail trips on trips.trip_id = a.list_id

;

commit;

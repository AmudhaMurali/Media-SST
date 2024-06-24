delete from &{pipeline_schema}.active_sponsored_trips_dwell_time_unique
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.active_sponsored_trips_dwell_time_unique
SELECT p.page as page_name
     , alb.servlet_name
     , COALESCE(cast(analytics.PUBLIC.regexp_substr_func(n.name,'TripDetails-t([0-9]*)', '1') as int),cast(analytics.PUBLIC.regexp_substr_func(n.name,'Trips\/([0-9]*)', '1') as int)) as trip_id
     , td.trip_title
     , td.username
     , td.display_name
     --, n.puid as page_id
     , alb.locale
     --, alb.page_action
     , alb.url
     , n.name as url_rum
     , alb.user_country_name
     , alb.os_type_name
     , alb.unique_id
     , m.value_ as dwell_time -- in milliseconds
     , alb.marketing_campaign_id
     , p.ds as ds

FROM rio_sf.rum.rum_page_loads p
JOIN WEB_PLATFORM.public.rum_metrics m on p.uid = m.puid
JOIN rio_sf.rum.rum_navigations n on p.uid = n.puid
JOIN (select  j.unique_id, j.uid, j.url, j.servlet_name, j.user_country_name, j.locale, j.os_type_name, j.ds , max(j.marketing_campaign_id) as marketing_campaign_id
      from user_tracking.public.a_lookback_blessed_joined j
      where j.ds >= '&{start_dt}' and j.ds <= '&{end_dt}'
      group by j.unique_id, j.uid, j.url, j.servlet_name, j.user_country_name, j.locale, j.os_type_name, j.ds
       ) alb on alb.uid = p.uid and alb.ds = p.ds
JOIN &{pipeline_schema}.active_sponsored_trips_detail td on , COALESCE(cast(analytics.PUBLIC.regexp_substr_func(n.name,'TripDetails-t([0-9]*)', '1') as int),cast(analytics.PUBLIC.regexp_substr_func(n.name,'Trips\/([0-9]*)', '1') as int)) = td.trip_id
WHERE p.ds >= '&{start_dt'
  AND p.ds <= '&{end_dt}'
  AND m.name = 'document-dwell-time'
  AND lower(p.page) like '%trip%' and alb.servlet_name in ('Trips', 'TripDetails')
;
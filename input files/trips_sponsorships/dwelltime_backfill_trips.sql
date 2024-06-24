delete from &{pipeline_schema}.dwelltime_backfill_trips
where ds between '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.dwelltime_backfill_trips
SELECT p.page as page_name
     , alb.servlet_name
     , COALESCE(try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'TripDetails-t([0-9]*)', '1') as int),try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'Trips\/([0-9]*)', '1') as int))  as trip_id
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
     , m.value as dwell_time -- in milliseconds
     , alb.marketing_campaign_id
     , p.ds as ds

FROM RIO_SF.RUM.RUM_PAGE_LOADS p
JOIN rio_sf.tripmix.tripmix_rum_metrics_archive m on p.uid = m.puid
JOIN RIO_SF.RUM.RUM_NAVIGATIONS n on p.uid = n.puid
JOIN (select  j.unique_id, j.uid, j.url, j.servlet_name, j.user_country_name, j.locale, j.os_type_name, j.ds , max(j.marketing_campaign_id) as marketing_campaign_id
      from user_tracking.public.a_lookback_blessed_joined j
      where j.ds >= '&{start_dt}' and j.ds <= '&{end_dt}'
      group by j.unique_id, j.uid, j.url, j.servlet_name, j.user_country_name, j.locale, j.os_type_name, j.ds
       ) alb on alb.uid = p.uid and alb.ds = p.ds
JOIN &{pipeline_schema}.active_sponsored_trips_detail td on COALESCE(try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'TripDetails-t([0-9]*)', '1') as int),try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'Trips\/([0-9]*)', '1') as int))  = td.trip_id
WHERE p.ds >= '&{start_dt}'
  AND p.ds <= '&{end_dt}'
  AND m.name = 'document-dwell-time'
  AND lower(p.page) like '%trips%'
  AND COALESCE(try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'TripDetails-t([0-9]*)', '1') as int),try_cast(analytics.PUBLIC.regexp_substr_func(n.name,'Trips\/([0-9]*)', '1') as int))  is not null
;
BEGIN;;
delete from &{pipeline_schema_sf}.infocenter_dwell_time_by_unique
where ds between '&{start_dt}' and '&{end_dt}';
;

insert into &{pipeline_schema_sf}.infocenter_dwell_time_by_unique
SELECT p.page as page_name
     , alb.servlet_name
     , n.puid as page_id
     , alb.locale
     , alb.page_action
     , alb.url
     , n.name as url_rum
     , alb.user_country_name
     , alb.os_type_name
     , alb.unique_id
     , m.value_ as dwell_time -- in milliseconds
     , p.ds as ds
     , regexp_substr(alb.url,'mcid?=[0-9]+') as mcid
FROM web_platform.public.rum_page_loads p
         JOIN WEB_PLATFORM.public.rum_metrics m on p.uid = m.puid
         JOIN web_platform.public.rum_navigations n on p.uid = n.puid
         JOIN (select distinct j.unique_id, j.uid, j.servlet_name, j.page_action, j.url, j.user_country_name, j.locale, j.os_type_name, j.ds
               from user_tracking.public.a_lookback_blessed_joined j
               where j.ds >= '&{start_dt}' and j.ds <= '&{end_dt}'
                 and lower(j.servlet_name) like '%infocenter%'
) alb on alb.uid = p.uid and alb.ds = p.ds
WHERE p.ds >= '&{start_dt}'
  AND p.ds <= '&{end_dt}'
  AND m.name = 'document-dwell-time'
  AND lower(p.page) like '%infocenter%'
  AND lower(n.name) like '%infocenter%'
;
commit;
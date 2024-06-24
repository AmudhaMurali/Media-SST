BEGIN;;
delete from &{pipeline_schema_sf}.sponsored_articles_dwell_time_unique
where ds between '&{start_dt}' and '&{end_dt}';
;

insert into &{pipeline_schema_sf}.sponsored_articles_dwell_time_unique
SELECT p.page as page_name
     , split_part(regexp_substr(n.name,'Articles-l([0-9a-zA-Z]*)'),'-l',2) as url_article_id
     , t.article_title
     , sa.sponsor_name
     , sa.use_sponsor_info
     , sa.locale
     , alb.url
     , n.name as url_rum
     , alb.user_country_name
     , alb.os_type_name
     , alb.unique_id
     , m.value_ as dwell_time -- in milliseconds
     , p.ds as ds
FROM WEB_PLATFORM.public.rum_page_loads p
JOIN WEB_PLATFORM.public.rum_metrics m on p.uid = m.puid and p.ds = m.ds
JOIN WEB_PLATFORM.public.rum_navigations n on p.uid = n.puid and p.ds = n.ds
LEFT JOIN &{pipeline_schema_sf}.article_title t on split_part(regexp_substr(n.name,'Articles-l([0-9a-zA-Z]*)'),'-l',2) = t.article_id
LEFT JOIN &{pipeline_schema_sf}.sponsored_articles sa on split_part(regexp_substr(n.name,'Articles-l([0-9a-zA-Z]*)'),'-l',2) = sa.space and p.LOCALE = sa.LOCALE
JOIN (select distinct j.unique_id, j.uid, j.url, j.user_country_name, j.os_type_name, j.ds
      from user_tracking.public.a_lookback_blessed_joined j
      where j.ds >= '&{start_dt}' and j.ds <= '&{end_dt}'
       ) alb on alb.uid = p.uid and alb.ds = p.ds
WHERE p.ds >= '&{start_dt}'
  AND p.ds <= '&{end_dt}'
  AND m.name = 'document-dwell-time'
  AND lower(p.page) like '%articles%'
;
commit;


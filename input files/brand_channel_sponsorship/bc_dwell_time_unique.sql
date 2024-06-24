delete from &{pipeline_schema_sf}.bc_dwell_time_unique
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_dwell_time_unique
SELECT alb.unique_id    as unique_id,
       alb.locale       as locale,
       alb.os_type_name as platform,
       p.page,
       cast(analytics.PUBLIC.regexp_substr_func(n.name,'-g([0-9]+)-', '1') as int) as bc_geo_id,
       loc.primaryname  as bc_geo_name,
       m.value_          as dwell_time, -- in milliseconds
       p.ds             as ds
FROM web_platform.public.rum_page_loads p
JOIN WEB_PLATFORM.public.rum_metrics m on p.uid = m.puid and p.ds = m.ds
JOIN web_platform.public.rum_navigations n on p.uid = n.puid and p.ds = n.ds
JOIN (select distinct j.unique_id, j.uid, j.locale, j.os_type_name, j.ds
          from user_tracking.public.a_lookback_blessed_joined j
         where j.ds >= '&{start_dt}' and j.ds <= '&{end_dt}'
          ) alb on alb.uid = p.uid and alb.ds = p.ds
JOIN (SELECT distinct location_id
      FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.T_SPOCK_BRANDCHANNELS
      WHERE ds >= '&{start_dt}' AND
        ds <= '&{end_dt}'
     ) a on cast(a.location_id as int) = cast(analytics.PUBLIC.regexp_substr_func(n.name,'-g([0-9]+)-', '1') as int) -- active brand channels
LEFT JOIN rio_sf.public.t_location loc ON loc.id = cast(analytics.PUBLIC.regexp_substr_func(n.name,'-g([0-9]+)-', '1') as int)
WHERE m.name = 'document-dwell-time' AND
     p.ds >= '&{start_dt}' AND
     p.ds <= '&{end_dt}' and
     p.page='Tourism'

;
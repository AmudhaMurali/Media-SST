begin;

delete from &{pipeline_schema_sf}.bc_dwell_time_agg
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_dwell_time_agg
with

dwell_time as (
         select unique_id,
                locale,
                platform,
                page,
                bc_geo_id,
                bc_geo_name,
                dwell_time,
                percentile_cont(0.75) within group(order by dwell_time) over (partition by bc_geo_id, ds) as p75,
                case when dwell_time <= p75 then 0 else 1 end as over_p75, -- identifies dwell times above 75th percentile
                ds
         from &{pipeline_schema_sf}.bc_dwell_time_unique
         where ds between '&{start_dt}' and '&{end_dt}'
    )

, uniques AS (
         SELECT ds,
               unique_id,
               marketing_campaign_id,
               user_country_id,
               user_country_name,
               op1_order_id,
               bc_geo_id
         FROM &{pipeline_schema_sf}.bc_unique
         WHERE ds between '&{start_dt}' and '&{end_dt}'
         GROUP BY ds,
               unique_id,
               marketing_campaign_id,
               user_country_id,
               user_country_name,
               op1_order_id,
               bc_geo_id
)

select count(distinct du.unique_id) as uniques
       , du.locale
       , du.platform
       , du.page
       , du.bc_geo_id
       , du.bc_geo_name
       , round((sum(du.dwell_time)/1000),2) as total_dwell_time -- OG in milliseconds, this is in sec (divide by 1,000 to get seconds; divide by 60,000 to get minutes)
       , du.ds
       , uu.marketing_campaign_id
       , uu.user_country_id
       , uu.user_country_name
       , uu.op1_order_id
FROM
  --&{pipeline_schema_sf}.bc_dwell_time_unique du
  dwell_time du
  LEFT JOIN uniques uu
    ON du.ds = uu.ds
    AND du.unique_id = uu.unique_id
    AND du.bc_geo_id = uu.bc_geo_id
WHERE du.ds between '&{start_dt}' and '&{end_dt}'
  AND over_p75 = 0 -- if we wanted to limit to under 75 percentile
GROUP BY du.locale
       , du.platform
       , du.page
       , du.bc_geo_id
       , du.bc_geo_name
       , du.ds
       , uu.marketing_campaign_id
       , uu.user_country_id
       , uu.user_country_name
       , uu.op1_order_id
;

commit;

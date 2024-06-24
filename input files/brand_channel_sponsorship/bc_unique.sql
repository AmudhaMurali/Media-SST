begin;

delete from &{pipeline_schema_sf}.bc_unique
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_unique

with bc_op_order_id as (
        SELECT DISTINCT --unique_id,
          locale, --old format en_US
          ds, geo_id, custom_data_json['operativeOneOrderIds'][0] as op1_order_id
        FROM user_tracking.public.user_impressions imp
        WHERE item_type = 'BrandChannelImpression'
        AND ds between '&{start_dt}' and '&{end_dt}'
)

--Brand Channel Shelf Impressions
select bcs.ds,
       bcs.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       bco.op1_order_id,
       bc_geo_id,
       bc_geo_name,
       os_type,
       bcs.locale, --old format en_US
       bc_shelf as element_type,
       to_boolean(case when sum(interactions) > 0 then 1 else 0 end) as had_interaction,
       sum(impressions) as impressions,
       sum(interactions) as interactions,
       null as video_completion_rate
from &{pipeline_schema_sf}.bc_shelf_spotlight_unique bcs
  INNER JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels geo --restrict to geos and locales active with BC
    ON bcs.bc_geo_id = geo.location_id
    AND bcs.locale = (Case when geo.old_locale = 'id' then 'in' else geo.old_locale end) --include local id, id locale saved as 'in' in usertracking tbl
    AND bcs.ds = geo.ds
  LEFT JOIN bc_op_order_id bco
    on bco.geo_id = bcs.bc_geo_id
    and bco.ds = bcs.ds
    --and bco.unique_id = bcs.unique_id
    and bco.locale = bcs.locale
where bcs.ds between '&{start_dt}' and '&{end_dt}'
and bc_shelf not in ('MAPPING_NOT_DEFINED')
group by bcs.ds,
       bcs.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       op1_order_id,
       bc_geo_id,
       bc_geo_name,
       os_type,
       bcs.locale,
       bc_shelf

UNION ALL

-- Brand Channel Video Impressions from IIS
select bcv.ds,
       bcv.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       op1_order_id,
       bc_geo_id,
       bc_geo_name,
       os_type,
       bcv.locale,  --old format en_US
       bc_shelf as element_type,
       to_boolean(case when sum(interactions) > 0 then 1 else 0 end) as had_interaction,
       sum(impressions) as impressions,
       sum(interactions) as interactions,
       max(video_completion_rate) as video_completion_rate
from &{pipeline_schema_sf}.bc_video_unique bcv
  INNER JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels geo --restrict to geos and locales active with BC
    ON bcv.bc_geo_id = geo.location_id
    AND bcv.locale =  (Case when geo.old_locale = 'id' then 'in' else geo.old_locale end) --include local id, id locale saved as 'in' in usertracking tbl
    AND bcv.ds = geo.ds
LEFT JOIN bc_op_order_id bco
  on bco.geo_id = bcv.bc_geo_id
  and bco.ds = bcv.ds
  --and bco.unique_id = bcv.unique_id
  and bco.locale = bcv.locale
where bcv.ds between '&{start_dt}' and '&{end_dt}'
group by bcv.ds,
       bcv.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       op1_order_id,
       bc_geo_id,
       bc_geo_name,
       os_type,
       bcv.locale,
       bc_shelf

UNION ALL

--Brand Channel DFP Ad Impressions
select bcd.ds,
       bcd.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       op1_order_id,
       bc_geo_id,
       bc_geo_name,
       device as os_type,
       bcd.locale, --old format en_US
       element_type,
       had_interaction,
       sum(impressions) as impressions,
       sum(interactions) as interactions,
       null as video_completion_rate
from &{pipeline_schema_sf}.bc_dfp_ads bcd
  INNER JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels geo --restrict to geos and locales active with BC
    ON bcd.bc_geo_id = geo.location_id
    AND bcd.locale = (Case when geo.old_locale = 'id' then 'in' else geo.old_locale end) --include local id, id locale saved as 'in' in usertracking tbl
    AND bcd.ds = geo.ds
  LEFT JOIN bc_op_order_id bco
    on bco.geo_id = bcd.bc_geo_id
    and bco.ds = bcd.ds
    --and bco.unique_id = bcd.unique_id
    and bco.locale = bcd.locale
where bcd.ds between '&{start_dt}' and '&{end_dt}'
group by bcd.ds,
       bcd.unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       op1_order_id,
       bc_geo_id,
       bc_geo_name,
       os_type,
       bcd.locale,
       element_type,
       had_interaction

;

commit;
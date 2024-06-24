-- Replacing old &{pipeline_schema}.Infocenter_pageviews_uniques and &{pipeline_schema}.Infocenter_GAadsol tables with new logic
-- Updayed July, 2021 pulling from lookback with UTM tracking parameters from URL and Order Id from Page Action


BEGIN;
DELETE FROM &{pipeline_schema_sf}.infocenter_reporting
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.infocenter_reporting
with icr1 as (
SELECT alpv.ds,
       alpv.unique_id,
       alpv.servlet_name,
       amt.new_page_action,
       alpv.page_action,
       coalesce(amt.new_page_action,alpv.page_action) as page_action_coalesce,
       alpv.url,
       regexp_substr(alpv.url,'source\\W+\\w+\\D\\w+') as utm_source,
       regexp_substr(alpv.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
       regexp_substr(alpv.url,'mcid?=[0-9]+') as mcid,
       alpv.last_referrer,
       alpv.last_referrer_domain,
       alpv.landing_page_referrer,
       alpv.landing_page,
       alpv.external_referral,
       alpv.client_type,
       alpv.os_type_id,
       alpv.os_type_name,
       alpv.user_country_id,
       alpv.user_country_name,
       alpv.pos,
       alpv.is_blessed
FROM rio_sf.anm.a_lookback_blessed_joined_only_pv alpv
LEFT JOIN display_ads.sales.advertorial_mapping_table amt on lower(alpv.page_action) = lower(amt.page_action)
WHERE alpv.ds between '&{start_dt}' and '&{end_dt}'
AND alpv.servlet_name = 'InfoCenterV6')

SELECT ds,
       unique_id,
       servlet_name,
       new_page_action,
       page_action,
       page_action_coalesce,
       url,
       utm_source,
       utm_medium,
       last_referrer,
       last_referrer_domain,
       landing_page_referrer,
       landing_page,
       external_referral,
       client_type,
       os_type_id,
       os_type_name,
       user_country_id,
       user_country_name,
       pos,
       is_blessed,
       null as utm_source_tran,
      case when icr1.mcid = 'mcid=66447' then 'Connect'
           when  icr1.mcid = 'mcid=66483' then 'Organic Social'
           when  icr1.mcid = 'mcid=66484' then 'Client or PR Traffic'
           when  icr1.mcid = 'mcid=66485' then 'Rove'
           when  icr1.mcid = 'mcid=66486' then 'Email'
           when  icr1.mcid = 'mcid=66487' then 'IAB or High Impact'
           when  icr1.mcid = 'mcid=66488' then 'ta-native'
           when  icr1.mcid = 'mcid=66489' then 'Boost'
           when  icr1.mcid = 'mcid=66490' then 'Video'
           when  icr1.mcid = 'mcid=66492' then 'content-discovery'
           when  icr1.mcid = 'mcid=66491' then 'TA Custom Content'
           when  icr1.mcid = 'mcid=66768' then 'SEM'
           when  icr1.mcid = 'mcid=67346' then 'Voice'
           when  icr1.mcid = 'mcid=67750' then 'Influencer'
           when  icr1.mcid = 'mcid=67749' then 'Podcasts'
        else null end as mcid_tran
from icr1

/*SELECT alpv.ds,
       alpv.unique_id,
       alpv.servlet_name,
       amt.new_page_action,
       alpv.page_action,
       coalesce(amt.new_page_action,alpv.page_action) as page_action_coalesce,
       alpv.url,
       regexp_substr(alpv.url,'source\\W+\\w+\\D\\w+') as utm_source,
       regexp_substr(alpv.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
       alpv.last_referrer,
       alpv.last_referrer_domain,
       alpv.landing_page_referrer,
       alpv.landing_page,
       alpv.external_referral,
       alpv.client_type,
       alpv.os_type_id,
       alpv.os_type_name,
       alpv.user_country_id,
       alpv.user_country_name,
       alpv.pos,
       alpv.is_blessed
FROM rio_sf.anm.a_lookback_blessed_joined_only_pv alpv
LEFT JOIN display_ads.sales.advertorial_mapping_table amt on lower(alpv.page_action) = lower(amt.page_action)
WHERE alpv.ds between '&{start_dt}' and '&{end_dt}'
AND alpv.servlet_name = 'InfoCenterV6'*/
;

commit;
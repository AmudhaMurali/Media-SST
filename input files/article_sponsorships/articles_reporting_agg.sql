
BEGIN;
DELETE FROM &{pipeline_schema_sf}.articles_reporting_agg
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.articles_reporting_agg
with operative_data as (
        select distinct op.op_advertiser_id,
                        op.advertiser_name,
                        op.sales_order_id,
                        op.sales_order_name,
                        op.industry,
                        ac.REGION,
                        op.account_exec
        from display_ads.sales.op1_line_items op
        left join display_ads.sales.op1_advertiser_country ac on ac.op_advertiser_id = op.op_advertiser_id
    ),

    pio_data as  (
        select distinct pio.advertiser_pio_id as op_advertiser_id,              --- on July 2023 OP1 was replaced by PIO. At the moment, we still name the columns with the ‘op’ prefix.
                        pio.advertiser_name,
                        COALESCE(piomap.op1_sales_order_id,pio.campaign_pio_id) as sales_order_id,
                        pio.campaign_name as sales_order_name,
                        pio.industry,
                        ac.region,
                        pio.owner_name as account_exec
       from display_ads.pio.pio_op1_data_shim pio
       left join display_ads.sales.pio_to_op1_order_id_mapping piomap on pio.campaign_pio_id = piomap.campaign_pio_id and pio.line_item_pio_id = piomap.pio_line_item_id
       left join display_ads.sales.op1_advertiser_country ac on ac.op_advertiser_id = pio.advertiser_pio_id
       where pio.ds BETWEEN '&{start_dt}' and '&{end_dt}'
    ),

    articles_reporting as(
        select imp.ds,
               imp.LOCALE,
               imp.OS_PLATFORM,
               imp.USER_AGENT,
               u.COMMERCE_COUNTRY_ID,
               u.USER_COUNTRY_NAME,
               imp.ITEM_ID,
               imp.ITEM_NAME,
               imp.ITEM_TYPE,
               null as  action_type,
               null as action_sub_type,
               case when cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int)  is null then h.sales_order_id
                    else cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int)
               end  as order_id,
               cast(split_part(regexp_substr(imp.custom_data,'Articles-l\\w*-'),'-',2) as string) as url_id,
               null as utm_source,
               --regexp_substr(utm.url,'source\\W+\\w+\\D\\w+') as utm_source,
               --regexp_substr(utm.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
               count(distinct imp.IMPRESSION_ID) as total_impression,
               count(distinct imp.UNIQUE_ID) as total_users,
               null as total_interaction
        from user_tracking.public.user_impressions imp
             left join RIO_SF.rust.a_unique_users u on imp.UNIQUE_ID = u.UNIQUE_ID and imp.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
             left join display_ads.sales.historical_articles_order_mapping h on cast(split_part(regexp_substr(imp.custom_data,'Articles-l\\w*-'),'-',2) as string)  = h.url_article_id
        where  imp.item_type in ('articleSponsorInfoImpression','articleLinkClick')  and imp.ds between '&{start_dt}' and '&{end_dt}'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

        union all

        select r.ds,
               r.LOCALE,
               r.OS_PLATFORM,
               r.USER_AGENT,
               u.COMMERCE_COUNTRY_ID,
               u.USER_COUNTRY_NAME,
               r.ITEM_ID,
               r.ITEM_NAME,
               r.ITEM_TYPE,
               r. action_type,
               r.action_sub_type,
               case when  cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int)  is null then h.sales_order_id
                    else  cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int)
               end  as order_id,
               cast(split_part(regexp_substr(r.custom_data,'Articles-l\\w*-'),'-',2) as string) as url_id,
               null as utm_source,
               --regexp_substr(utm.url,'source\\W+\\w+\\D\\w+') as utm_source,
               --regexp_substr(utm.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
               null as total_impression,
               null as total_users,
               count(distinct r.INTERACTION_ID) as total_interaction
        from user_tracking.public.USER_INTERACTIONS r
             left join RIO_SF.rust.a_unique_users u on r.UNIQUE_ID = u.UNIQUE_ID and r.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
             left join display_ads.sales.historical_articles_order_mapping h on cast(split_part(regexp_substr(r.custom_data,'Articles-l\\w*-'),'-',2) as string) = h.url_article_id
        where  r.item_type in ('articleSponsorInfoImpression','articleLinkClick')  and r.ds between '&{start_dt}' and '&{end_dt}'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    )


select a.ds,
       a.url_id,
       op.op_advertiser_id,
       op.advertiser_name,
       a.order_id,
       op.sales_order_name,
       op.industry,
       op.region as advertiser_region,
       op.account_exec,
       sa.article_title,
       case when sa.sponsor_name is not null and sa.use_sponsor_info = 'true' then 'Sponsored' else 'Non-Sponsored' end as sponsor_status,
       sa.sponsor_name,
       sa.use_sponsor_info,
       a.utm_source,
       sa.locale,
       a.user_country_name,
       case when a.OS_PLATFORM like '%app%' then 'app'
           when a.OS_PLATFORM in ('%tablet%', '%ipad%') then 'tablet'
           when a.OS_PLATFORM in ('iphone_browser', 'android_browser') then 'mobile web'
           when a.OS_PLATFORM in ('linux', 'windows', 'osx') then 'desktop'
           when a.OS_PLATFORM is null then ''
           else 'other' end as platform,
       sa.campaign_start_date,
       sa.campaign_end_date,
       a.item_id,
       a.item_name,
       a.item_type,
       a.action_type,
       a.action_sub_type,
       a.total_impression,
       a.total_users,
       a.total_interaction
from articles_reporting a
left join &{pipeline_schema_sf}.sponsored_articles sa on sa.space = a.item_id and a.LOCALE = replace(sa.LOCALE,'-','_')
left join (select * from operative_data where '&{start_dt}' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
           union all
           select * from pio_data where '&{start_dt}' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date))  op on op.sales_order_id = a.order_id
where a.ds between '&{start_dt}' and '&{end_dt}'
;

commit;
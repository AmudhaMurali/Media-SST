-- Aggregated dwell time reporting table to be used in dashboard
-- Updated February, 2022 pulling from PIO (new billing system). Before that date, we were pulling from Op1 (old billing system)

BEGIN;
DELETE FROM &{pipeline_schema_sf}.sponsored_articles_dwell_time_agg
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.sponsored_articles_dwell_time_agg
with    op1_details as (
                   select distinct   ac.account_id  as op_advertiser_id,
                                     ac.account_name as advertiser_name,
                                     ac.industry as industry,
                                     so.sales_order_id,
                                     so.sales_order_name,
                                     soli.sales_order_line_item_id,
                                     soli.sales_order_line_item_name
                   FROM display_ads.operative_one.sales_order_line_items soli
                   LEFT JOIN display_ads.operative_one.sales_order so on soli.sales_order_id = so.sales_order_id
                                                                                                 and soli.ds = so.ds
                   LEFT JOIN display_ads.operative_one.accounts ac on ac.account_id = so.advertiser_id
                                                                                                 and ac.ds = so.ds
                   WHERE soli.ds = '2023-07-31'
                   AND DATE ('&{end_dt}') >= (SELECT MAX(pio_go_live_date) FROM display_ads.sales.pio_go_live_date)
    ),  operative_data as (
        select distinct op.op_advertiser_id,
                        op.advertiser_name,
                        op.sales_order_id,
                        op.sales_order_name,
                        op.industry,
                        ac.REGION,
                        op.account_exec
        from display_ads.sales.op1_line_items op
        left join (select OP_ADVERTISER_ID, max(REGION) as REGION from display_ads.sales.op1_advertiser_country group by 1) ac on ac.op_advertiser_id = op.op_advertiser_id
    ),

pio_data as  (
        select distinct -- Antonio updated on 12/13/2023 - looks for OP1 Advertiser ID first
                        COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id) as op_advertiser_id,              --- on July 2023 OP1 was replaced by PIO. At the moment, we still name the columns with the ‘op’ prefix.
                        -- Antonio updated on 04/12/2023 - looks for OP1 Advertiser Name first
                        COALESCE(op1.advertiser_name,op2.advertiser_name, CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                         THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                        ELSE pio.advertiser_name END) as advertiser_name,
                        -- Antonio updated on 12/13/2023 - looks for OP1 Order ID first
                        cast(coalesce (pto.op1_sales_order_id, op.op1_sales_order_id, pio.campaign_number, pio2.campaign_number, pio.campaign_pio_id) as int) sales_order_id,
                        -- Antonio updated on 12/13/2023 - looks for OP1 Order name first
                        COALESCE(op1.sales_order_name, op2.sales_order_name, pio.campaign_name) as sales_order_name,
                        COALESCE(op1.industry,op2.industry,pio.industry) as industry,
                        ac.region,
                        pio.owner_name as account_exec
       from display_ads.pio.pio_op1_data_shim pio
       left join display_ads.sales.pio_to_op1_order_id_mapping pto on pio.campaign_pio_id = pto.campaign_pio_id and pio.line_item_pio_id = pto.pio_line_item_id
        -- Antonio updated on 12/13/2023 - New join to retrieve OP1 details at line level
            LEFT JOIN op1_details as op1
                ON pto.op1_sales_order_line_item_id = op1.sales_order_line_item_id
            -- Antonio updated on 12/13/2023 - New join to retrieve PIO Campaign Number for line in period between August 1st and September 11th
            LEFT JOIN (
                        SELECT distinct campaign_number,
                                        campaign_pio_id
                        FROM display_ads.pio.PIO_OP1_DATA_SHIM
                        WHERE campaign_number is not null
                        ) pio2
                ON pio.campaign_pio_id = pio2.campaign_pio_id
            -- Antonio updated on 12/13/2023 - New join to bring OP1 order ID for legacy campaigns
            LEFT JOIN (
                        SELECT distinct campaign_pio_id,
                                        op1_sales_order_id
                        FROM  display_ads.sales.pio_to_op1_order_id_mapping
                        ) op
                ON pio.campaign_pio_id = op.campaign_pio_id
            -- Antonio updated on 12/13/2023 - New join to bring OP1 details at Order level
            LEFT JOIN (
                        SELECT distinct op_advertiser_id,
                                        advertiser_name,
                                        industry,
                                        sales_order_id,
                                        sales_order_name
                        FROM op1_details
                        WHERE sales_order_id IS NOT NULL
                       ) op2
                ON op.op1_sales_order_id = op2.sales_order_id
       left join (select OP_ADVERTISER_ID, max(REGION) as REGION from display_ads.sales.op1_advertiser_country group by 1) ac on ac.op_advertiser_id = COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id)
       where pio.ds BETWEEN '&{start_dt}' and '&{end_dt}'
            and pio.line_item_delivery_status != 'not_pushed'
            and pio.organization_id_advertiser != 'Cruise Critic'
    ),
-- Antonio added pio 12/13/2023 - (New CTE) to guarantee that all details flow in even for legacy OP1 orders that using PIO details
pio as  (
        select distinct -- Antonio updated on 12/13/2023 - looks for OP1 Advertiser ID first
                        COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id) as op_advertiser_id,              --- on July 2023 OP1 was replaced by PIO. At the moment, we still name the columns with the ‘op’ prefix.
                        -- Antonio updated on 04/12/2023 - looks for OP1 Advertiser Name first
                        COALESCE(op1.advertiser_name,op2.advertiser_name, CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                         THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                        ELSE pio.advertiser_name END) as advertiser_name,
                        -- Antonio updated on 12/13/2023 - looks for OP1 Order ID first
                        coalesce(pio.campaign_number, pio2.campaign_number, pio.campaign_pio_id) as sales_order_id,
                        pio.campaign_pio_id,
                        coalesce(pto.op1_sales_order_id, op.op1_sales_order_id) as op1_sales_order_id,
                        -- Antonio updated on 12/13/2023 - looks for OP1 Order name first
                        COALESCE(op1.sales_order_name, op2.sales_order_name, pio.campaign_name) as sales_order_name,
                        COALESCE(op1.industry,op2.industry,pio.industry) as industry,
                        ac.region,
                        pio.owner_name as account_exec
       from display_ads.pio.pio_op1_data_shim pio
       left join display_ads.sales.pio_to_op1_order_id_mapping pto on pio.campaign_pio_id = pto.campaign_pio_id and pio.line_item_pio_id = pto.pio_line_item_id
        -- Antonio updated on 12/13/2023 - New join to retrieve OP1 details at line level
            LEFT JOIN op1_details as op1
                ON pto.op1_sales_order_line_item_id = op1.sales_order_line_item_id
            -- Antonio updated on 12/13/2023 - New join to retrieve PIO Campaign Number for line in period between August 1st and September 11th
            LEFT JOIN (
                        SELECT distinct campaign_number,
                                        campaign_pio_id
                        FROM display_ads.pio.PIO_OP1_DATA_SHIM
                        WHERE campaign_number is not null
                        ) pio2
                ON pio.campaign_pio_id = pio2.campaign_pio_id
            -- Antonio updated on 12/13/2023 - New join to bring OP1 order ID for legacy campaigns
            LEFT JOIN (
                        SELECT distinct campaign_pio_id,
                                        op1_sales_order_id
                        FROM  display_ads.sales.pio_to_op1_order_id_mapping
                        ) op
                ON pio.campaign_pio_id = op.campaign_pio_id
            -- Antonio updated on 12/13/2023 - New join to bring OP1 details at Order level
            LEFT JOIN (
                        SELECT distinct op_advertiser_id,
                                        advertiser_name,
                                        industry,
                                        sales_order_id,
                                        sales_order_name
                        FROM op1_details
                        WHERE sales_order_id IS NOT NULL
                       ) op2
                ON op.op1_sales_order_id = op2.sales_order_id
        left join (select OP_ADVERTISER_ID, max(REGION) as REGION from display_ads.sales.op1_advertiser_country group by 1) ac on ac.op_advertiser_id = COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id)
       where pio.ds BETWEEN '&{start_dt}' and '&{end_dt}'
            and pio.line_item_delivery_status != 'not_pushed'
            and pio.organization_id_advertiser != 'Cruise Critic'
    ),

     articles_dwell_time as (
         select dt.ds,
                dt.unique_id,
                dt.url_article_id,
                concat('l',dt.url_article_id) as url_id,
                dt.article_title,
                case when dt.sponsor_name = '' then null else dt.sponsor_name end as sponsor_name,
                dt.use_sponsor_info,
                split_part(split_part(regexp_substr(dt.url,'source\\W+\\w+\\D\\w+'),'=',2),'&&',1) as utm_source,
                regexp_substr(dt.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
                case when map.order_id is null then h.sales_order_id
                    else map.order_id
                end as order_id,
                dt.locale,
                dt.user_country_name,
                dt.os_type_name,
                dt.dwell_time, -- milliseconds
                percentile_cont(0.75) within group(order by dt.dwell_time) over (partition by dt.article_title, dt.ds) as p75,
                case when dt.dwell_time <= p75 then 0 else 1 end as over_p75
         from &{pipeline_schema_sf}.sponsored_articles_dwell_time_unique dt
         left join (select distinct order_id, url_id from &{pipeline_schema_sf}.article_order_unique_id_mapping) map on map.url_id = concat('l',dt.url_article_id)
         left join display_ads.sales.historical_articles_order_mapping h on concat('l',dt.url_article_id) = h.url_article_id
         where dt.ds between '&{start_dt}' and '&{end_dt}'
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
       a.article_title,
       case when a.order_id is not null  then 'Sponsored' else 'Non-Sponsored' end as sponsor_status,
       a.sponsor_name,
       a.use_sponsor_info,
       a.utm_source,
       a.locale,
       a.user_country_name,
       case when a.os_type_name like '%app%' then 'app'
           when a.os_type_name in ('%tablet%', '%ipad%') then 'tablet'
           when a.os_type_name in ('iphone_browser', 'android_browser') then 'mobile web'
           when a.os_type_name in ('linux', 'windows', 'osx') then 'desktop'
           when a.os_type_name is null then ''
           else 'other' end as platform,
       count(distinct a.unique_id) as uniques,
       sum(a.dwell_time) as total_dwell_time_ms, -- in milliseconds
       avg(a.dwell_time) as avg_dwell_time_ms -- in milliseconds
from articles_dwell_time a
left join (
    select sales_order_id,  max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from
  (select * from operative_data where '2023-12-01' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
           union all
   select * from pio_data where '2023-12-01' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date))
    Group by 1
    ) op on op.sales_order_id = a.order_id
--left join on pio number 4 digit
left join (select sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from pio
    group by 1,2) pio
        on pio.sales_order_id = a.order_id
--left join on pio number 4 digit--
left join (select campaign_pio_id,sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from pio
    group by 1,2,3) pio2
        on pio2.campaign_pio_id = a.order_id
where a.ds between '&{start_dt}' and '&{end_dt}' and a.url_id is not null
and a.over_p75 = 0 -- filters out any dwell times above 75th percentile
group by a.ds, a.url_id, op.op_advertiser_id, op.advertiser_name, a.order_id, op.sales_order_name, op.industry, op.region, op.account_exec, a.article_title,
         a.sponsor_name, a.use_sponsor_info, a.utm_source, a.locale, a.user_country_name, platform, sponsor_status
;

commit;

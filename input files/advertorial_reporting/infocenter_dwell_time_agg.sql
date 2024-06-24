-- Updating dwell time table to RUM table and joining to all relevant operative one tables


BEGIN;
DELETE FROM &{pipeline_schema_sf}.infocenter_dwell_time_agg
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.infocenter_dwell_time_agg
with  op1_details as (
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
                   AND DATE('&{end_dt}') >= (SELECT MAX(pio_go_live_date) FROM display_ads.sales.pio_go_live_date)
    ),
    operative_data as (
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
        select distinct -- looks for OP1 Advertiser ID first
                        COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id) as op_advertiser_id,
                        --  looks for OP1 Advertiser Name first
                        COALESCE(op1.advertiser_name,op2.advertiser_name, CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                         THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                        ELSE pio.advertiser_name END) as advertiser_name,
                        --  looks for OP1 Order ID first
                        cast(coalesce (pto.op1_sales_order_id, op.op1_sales_order_id, pio.campaign_number, pio2.campaign_number, pio.campaign_pio_id) as int) sales_order_id,
                        --  looks for OP1 Order name first
                        COALESCE(op1.sales_order_name, op2.sales_order_name, pio.campaign_name) as sales_order_name,
                        COALESCE(op1.industry,op2.industry,pio.industry) as industry,
                        ac.region,
                        pio.owner_name as account_exec
       from display_ads.pio.pio_op1_data_shim pio
       left join display_ads.sales.pio_to_op1_order_id_mapping pto on pio.campaign_pio_id = pto.campaign_pio_id and pio.line_item_pio_id = pto.pio_line_item_id
        --  New join to retrieve OP1 details at line level
       LEFT JOIN op1_details as op1 ON pto.op1_sales_order_line_item_id = op1.sales_order_line_item_id
            --  New join to retrieve PIO Campaign Number for line in period between August 1st and September 11th
       LEFT JOIN (SELECT distinct campaign_number,campaign_pio_id FROM display_ads.pio.PIO_OP1_DATA_SHIM WHERE campaign_number is not null) pio2 ON pio.campaign_pio_id = pio2.campaign_pio_id
            -- New join to bring OP1 order ID for legacy campaigns
       LEFT JOIN (SELECT distinct campaign_pio_id,op1_sales_order_id FROM  display_ads.sales.pio_to_op1_order_id_mapping ) op ON pio.campaign_pio_id = op.campaign_pio_id
            --  New join to bring OP1 details at Order level
       LEFT JOIN ( SELECT distinct op_advertiser_id,advertiser_name,industry,sales_order_id,sales_order_name FROM op1_details WHERE sales_order_id IS NOT NULL) op2
                ON op.op1_sales_order_id = op2.sales_order_id
       left join (select OP_ADVERTISER_ID, max(REGION) as REGION from display_ads.sales.op1_advertiser_country group by 1) ac on ac.op_advertiser_id = COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id)
       where pio.ds BETWEEN '&{start_dt}' and '&{end_dt}'
        and pio.line_item_delivery_status != 'not_pushed'
        and pio.organization_id_advertiser != 'Cruise Critic'
    ),
-- (New CTE) to guarantee that all details flow in even for legacy OP1 orders that using PIO details
pio as  (
        select distinct --  looks for OP1 Advertiser ID first
                        COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id) as op_advertiser_id,              --- on July 2023 OP1 was replaced by PIO. At the moment, we still name the columns with the 'op' prefix.
                        -- looks for OP1 Advertiser Name first
                        COALESCE(op1.advertiser_name,op2.advertiser_name, CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                         THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                        ELSE pio.advertiser_name END) as advertiser_name,
                        -- looks for OP1 Order ID first
                        coalesce(pio.campaign_number, pio2.campaign_number, pio.campaign_pio_id) as sales_order_id,
                        pio.campaign_pio_id,
                        coalesce(pto.op1_sales_order_id, op.op1_sales_order_id) as op1_sales_order_id,
                        -- looks for OP1 Order name first
                        COALESCE(op1.sales_order_name, op2.sales_order_name, pio.campaign_name) as sales_order_name,
                        COALESCE(op1.industry,op2.industry,pio.industry) as industry,
                        ac.region,
                        pio.owner_name as account_exec
       from display_ads.pio.pio_op1_data_shim pio
       left join display_ads.sales.pio_to_op1_order_id_mapping pto on pio.campaign_pio_id = pto.campaign_pio_id and pio.line_item_pio_id = pto.pio_line_item_id
        --  New join to retrieve OP1 details at line level
        LEFT JOIN op1_details as op1 ON pto.op1_sales_order_line_item_id = op1.sales_order_line_item_id
        --  New join to retrieve PIO Campaign Number for line in period between August 1st and September 11th
        LEFT JOIN (SELECT distinct campaign_number, campaign_pio_id FROM display_ads.pio.PIO_OP1_DATA_SHIM WHERE campaign_number is not null) pio2 ON pio.campaign_pio_id = pio2.campaign_pio_id
        --  New join to bring OP1 order ID for legacy campaigns
        LEFT JOIN (SELECT distinct campaign_pio_id,op1_sales_order_id FROM  display_ads.sales.pio_to_op1_order_id_mapping) op ON pio.campaign_pio_id = op.campaign_pio_id
        --  New join to bring OP1 details at Order level
        LEFT JOIN (SELECT distinct op_advertiser_id,advertiser_name,industry,sales_order_id,sales_order_name FROM op1_details WHERE sales_order_id IS NOT NULL ) op2
          ON op.op1_sales_order_id = op2.sales_order_id
        left join (select OP_ADVERTISER_ID, max(REGION) as REGION from display_ads.sales.op1_advertiser_country group by 1) ac on ac.op_advertiser_id = COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id)
        where pio.ds BETWEEN '&{start_dt}' and '&{end_dt}'
            and pio.line_item_delivery_status != 'not_pushed'
            and pio.organization_id_advertiser != 'Cruise Critic'
    ),

    advertorial_dwell_time as (
        select icdt.ds,
               icdt.unique_id,
               split_part(split_part(regexp_substr(icdt.url,'source\\W+\\w+\\D\\w+'),'=',2),'&&',1) as utm_source,
               regexp_substr(icdt.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
               case when icdt.mcid = 'mcid=66447' then 'Connect'
                when  icdt.mcid = 'mcid=66483' then 'Organic Social'
                when  icdt.mcid = 'mcid=66484' then 'Client or PR Traffic'
                when  icdt.mcid = 'mcid=66485' then 'Rove'
                when  icdt.mcid = 'mcid=66486' then 'Email'
                when  icdt.mcid = 'mcid=66487' then 'IAB or High Impact'
                when  icdt.mcid = 'mcid=66488' then 'ta-native'
                when  icdt.mcid = 'mcid=66489' then 'Boost'
                when  icdt.mcid = 'mcid=66490' then 'Video'
                when  icdt.mcid = 'mcid=66492' then 'content-discovery'
                when  icdt.mcid = 'mcid=66491' then 'TA Custom Content'
                when  icdt.mcid = 'mcid=66768' then 'SEM'
                when  icdt.mcid = 'mcid=67346' then 'Voice'
                when  icdt.mcid = 'mcid=67750' then 'Influencer'
                when  icdt.mcid = 'mcid=67749' then 'Podcasts'
                else null end as mcid_tran,
               icdt.servlet_name,
               coalesce(amt.new_page_action,icdt.page_action) as page_action_coalesce,
               cast(rtrim(regexp_substr((coalesce(amt.new_page_action,icdt.page_action)),'^[0-9]+[_.]'),'_') as int) as order_id,
               icdt.locale,
               icdt.user_country_name,
               icdt.os_type_name,
               icdt.dwell_time, -- milliseconds
               percentile_cont(0.75) within group(order by icdt.dwell_time) over (partition by page_action_coalesce, ds) as p75,
               case when icdt.dwell_time <= p75 then 0 else 1 end as over_p75
        from &{pipeline_schema_sf}.infocenter_dwell_time_by_unique icdt
        left join display_ads.sales.advertorial_mapping_table amt on lower(icdt.page_action) = lower(amt.page_action)
        where icdt.ds between '&{start_dt}' and '&{end_dt}'
    )

select a.ds,
       a.servlet_name,
       a.page_action_coalesce as display_name,
       COALESCE(op.op_advertiser_id,pio.op_advertiser_id,pio2.op_advertiser_id)                                                                                                                      as op_advertiser_id,
       COALESCE(op.advertiser_name,pio.advertiser_name,pio2.advertiser_name)                                                                                                                         as advertiser_name,
       COALESCE(op.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,cast(rtrim(regexp_substr(a.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)) as order_id,
       COALESCE(op.sales_order_name,pio.sales_order_name,pio2.sales_order_name)                                                                                                                      as sales_order_name,
       COALESCE(op.industry,pio.industry,pio2.industry)                                                                                                                                              as industry,
       COALESCE(op.REGION,pio.REGION,pio2.REGION)                                                                                                                                                    as advertiser_region,
       COALESCE(op.account_exec,pio.account_exec,pio2.account_exec)                                                                                                                                  as account_exec,
       a.utm_source,
       a.locale,
       a.user_country_name,
       a.os_type_name,
       count(distinct a.unique_id) as uniques,
       sum(a.dwell_time) as total_dwell_time, -- in milliseconds
       avg(a.dwell_time) as avg_dwell_time, -- in milliseconds
       a.mcid_tran
from advertorial_dwell_time a
left join (select * from operative_data where '&{start_dt}' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
           UNION ALL
           select * from pio_data pio where '&{start_dt}' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)) op
on op.sales_order_id = cast(rtrim(regexp_substr(a.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
left join pio
        on pio.sales_order_id = cast(rtrim(regexp_substr(a.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
left join pio pio2
        on pio2.campaign_pio_id = cast(rtrim(regexp_substr(a.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
where a.ds between '&{start_dt}' and '&{end_dt}'
and a.over_p75 = 0 -- filters out any dwell times above 75th percentile
group by all
;

commit;
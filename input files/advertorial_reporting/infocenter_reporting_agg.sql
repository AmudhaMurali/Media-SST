-- Replacing old &{pipeline_schema}.Infocenter_pageviews_uniques and &{pipeline_schema}.Infocenter_GAadsol tables with new logic
-- Updayed July, 2021 pulling from lookback with UTM tracking parameters from URL and Order Id from Page Action

BEGIN;
DELETE FROM &{pipeline_schema_sf}.infocenter_reporting_agg
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.infocenter_reporting_agg
-- (New CTE) we want to retrieve advertiser name, order name and line item name from OP1 given that OP1 has precedence over PIO
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
    )

select icr.ds,
       icr.servlet_name,
       icr.page_action_coalesce as display_name,
       COALESCE(op.op_advertiser_id,pio.op_advertiser_id,pio2.op_advertiser_id)                                                                                                                      as op_advertiser_id,
       COALESCE(op.advertiser_name,pio.advertiser_name,pio2.advertiser_name)                                                                                                                         as advertiser_name,
       COALESCE(op.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)) as order_id,
       COALESCE(op.sales_order_name,pio.sales_order_name,pio2.sales_order_name)                                                                                                                      as sales_order_name,
       COALESCE(op.industry,pio.industry,pio2.industry)                                                                                                                                              as industry,
       COALESCE(op.REGION,pio.REGION,pio2.REGION)                                                                                                                                                    as advertiser_region,
       COALESCE(op.account_exec,pio.account_exec,pio2.account_exec)                                                                                                                                  as account_exec,
       split_part(split_part(icr.utm_source,'=',2),'&&',1) as utm_source,
       icr.client_type,
       icr.user_country_name,
       icr.pos as locale,
       count(distinct icr.unique_id) as uniques,
       count(1) as pageviews,
       icr.mcid_tran as traffic_source
from &{pipeline_schema_sf}.infocenter_reporting icr
left join (select * from operative_data where '&{start_dt}' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
           UNION ALL
           select * from pio_data pio where '&{start_dt}' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)) op
on op.sales_order_id = cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
left join pio
        on pio.sales_order_id = cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
left join pio pio2
        on pio2.campaign_pio_id = cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+[_.]'),'_') as int)
where icr.ds between '&{start_dt}' and '&{end_dt}'
group by all
;

commit;
/*BEGIN;
DELETE FROM &{pipeline_schema_sf}.infocenter_reporting_agg
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.infocenter_reporting_agg

/*select icr.ds,
       icr.servlet_name,
       icr.page_action_coalesce as display_name,
       op.op_advertiser_id,
       op.advertiser_name,
       cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+_'),'_') as int) as order_id,
       op.sales_order_name,
       op.industry,
       ac.region as advertiser_region,
       op.account_exec,
       split_part(split_part(icr.utm_source,'=',2),'&&',1) as utm_source,
       icr.client_type,
       icr.user_country_name,
       icr.pos as locale,
       count(distinct icr.unique_id) as uniques,
       count(1) as pageviews
from &{pipeline_schema_sf}.infocenter_reporting icr
left join operative_data op on op.sales_order_id = cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+_'),'_') as int)
left join &{pipeline_schema_sf}.op1_advertiser_country ac on ac.op_advertiser_id = op.op_advertiser_id
where icr.ds between '&{start_dt}' and '&{end_dt}'
group by icr.ds, icr.servlet_name, icr.page_action_coalesce, op.op_advertiser_id, op.advertiser_name,
         cast(rtrim(regexp_substr(icr.page_action_coalesce,'^[0-9]+_'),'_') as int), op.sales_order_name,
         op.industry, ac.region, op.account_exec, split_part(split_part(icr.utm_source,'=',2),'&&',1),
         icr.client_type, icr.user_country_name, icr.pos*/
;

commit;
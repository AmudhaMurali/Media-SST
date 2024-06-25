
-- join advertiser_spend table (from operative one joined to dfp) to economic impact advertisers --
--
begin;

delete from &{pipeline_schema}.mei_advertiser_spend;

insert into &{pipeline_schema}.mei_advertiser_spend
select distinct rwc.ds,
       rwc.month_and_year,
       oli.op_advertiser_id,
       rwc.advertiser_name                                              as op_advertiser_name,
       oli.ad_server,
       oli.ad_server_advertiser_id,
       dfp.advertiser_name,
       dfp.ad_name_formatted                                            as ad_name_formatted,
       rwc.category,
       oli.forecast_category,
       oli.sales_order_id,
       oli.sales_order_name,
       oli.sales_order_line_item_id,
       oli.sales_order_line_item_name,
       oli.line_item_start_date,
       oli.line_item_end_date,
       rwc.currency                                                     as currency_local_rev,
       rwc.local_revenue,
       rwc.usd_revenue,
       oli.currency_code                                                as currency_local_contract,
       oli.local_contracted_amount,
       oli.usd_contracted_amount
from  rio_sf.sales.t_operativeone_revenue_daily_with_children rwc
left join display_ads.sales.op1_line_items oli on rwc.line_item_id = oli.sales_order_line_item_id
LEFT JOIN &{pipeline_schema}.dfp_ad_name dfp
                ON oli.ad_server_advertiser_id = dfp.advertiser_id
where rwc.ds = '2023-07-31'--aug1 pio became live
  AND oli.line_item_end_date < '2023-08-01'

UNION ALL


select distinct rwc.ds,
       cast(concat(rwc.DATA_YEAR,'-',rwc.DATA_MONTH,'-01') as date)   as month_and_year ,
       oli.account_id_pio_advertiser                                  as op_advertiser_id,
       rwc.advertiser_name                                            as op_advertiser_name,
       oli.production_system_name                                     as ad_server, --NEEDS TO CONFIRM
       oli.advertiser_id                                              as ad_server_advertiser_id,
       dfp.advertiser_name                                            as ad_server_advertiser_name,
       dfp.ad_name_formatted                                          as ad_name_formatted,
       rwc.industry,
       oli.forecast_category,
       COALESCE(pto.op1_sales_order_id,oli.sales_order_id)            as sales_order_id,
       oli.sales_order_name,
       COALESCE(pto.op1_sales_line_item_id,rwc.line_item_pio_id)      as sales_order_line_item_id,
       oli.sales_order_line_item_name,
       rwc.line_item_start_date,
       rwc.line_item_end_date,
       rwc.CURRENCY_CODE                                                     as currency_local_rev,
       round(rwc.report_period_net_rev_recognition,2) as report_period_net_rev_recognition,
       round(rwc.report_period_net_rev_recognition * 1/cc.daily_currency_rate,2) as usd_revenue,
       oli.currency_code                                                as currency_local_contract,
       oli.net_cost,
       oli.net_cost * jan_rate AS usd_contracted_amount
from DISPLAY_ADS.PIO.PIO_OP1_REV_MNG_DATA_SHIM rwc
             LEFT JOIN ANALYTICS.PUBLIC.PIO_OP1_JOINT_MAPPINGS pto
                       ON cast(rwc.line_item_pio_id as int) = cast(pto.pio_line_item_id as int)
             left join DISPLAY_ADS.PIO.VW_PIO_OP1_DATA_SHIM_OP1_FIELD_NAMES oli
                       on rwc.LINE_ITEM_PIO_ID = oli.sales_order_line_item_id
                           AND rwc.ds = oli.ds
             LEFT JOIN &{pipeline_schema}.dfp_ad_name dfp
                ON oli.advertiser_id = dfp.advertiser_id
             LEFT JOIN RIO_SF.B2B_CORE.CURRENCY_DAILY cc
                       ON cc.currency_code = rwc.currency_code
                           AND cc.ds = (case when year (date (rwc.ds)) = 2022 then '2021-12-15'
                                                 when year(date(rwc.ds))>=2023 then '2022-12-05'
                                                 end)
where rwc.ds = (select max(ds) from DISPLAY_ADS.PIO.VW_PIO_OP1_DATA_SHIM_OP1_FIELD_NAMES)
and op_advertiser_name is not null
AND rwc.ds >= (SELECT MAX(pio_go_live_date) FROM display_ads.sales.pio_go_live_date) --PIO live
AND rwc.line_item_pio_id IS NOT NULL
AND rwc.line_item_end_date >= '2023-08-01'

;

--update

-- join advertiser_spend table (from operative one joined to dfp) to economic impact advertisers --

-- begin;
--
-- delete from &{pipeline_schema}.mei_advertiser_spend;
--
-- insert into &{pipeline_schema}.mei_advertiser_spend
-- select distinct rwc.ds,
--        rwc.month_and_year,
--        oli.op_advertiser_id,
--        rwc.advertiser_name                                              as op_advertiser_name,
--        oli.ad_server,
--        oli.ad_server_advertiser_id,
--        oli.ad_server_advertiser_name,
--        replace(substr(oli.ad_server_advertiser_name,4,100),'_', ' ')    as ad_name_formatted,
--        rwc.category,
--        oli.forecast_category,
--        oli.sales_order_id,
--        oli.sales_order_name,
--        oli.sales_order_line_item_id,
--        oli.sales_order_line_item_name,
--        oli.line_item_start_date,
--        oli.line_item_end_date,
--        rwc.currency                                                     as currency_local_rev,
--        rwc.local_revenue,
--        rwc.usd_revenue,
--        oli.currency_code                                                as currency_local_contract,
--        oli.local_contracted_amount,
--        oli.usd_contracted_amount
-- from  rio_sf.sales.t_operativeone_revenue_daily_with_children rwc
-- left join display_ads.sales.op1_line_items oli on rwc.line_item_id = oli.sales_order_line_item_id
-- where rwc.ds = (select max(ds) from rio_sf.sales.t_operativeone_revenue_daily_with_children)
-- ;

commit;
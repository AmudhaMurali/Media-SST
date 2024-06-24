DELETE FROM &{pipeline_schema}.article_shelf WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';



INSERT INTO &{pipeline_schema_sf}.article_shelf
SELECT
        a.ds,
        a.url_id,
        op.op_advertiser_id,
        op.advertiser_name,
        a.order_id,
        op.sales_order_name,
        op.industry,
        op.region as advertiser_region,
        op.account_exec,
        t.article_title,
        case when a.order_id is not null  then 'Sponsored' else 'Non-Sponsored' end as sponsor_status,
        sa.sponsor_name,
        sa.use_sponsor_info,
        a.utm_source,
        a.locale,
        a.user_country_name,
        case when a.OS_PLATFORM like '%app%' then 'app'
        when a.OS_PLATFORM in ('%tablet%', '%ipad%') then 'tablet'
        when a.OS_PLATFORM in ('iphone_browser', 'android_browser') then 'mobile web'
        when a.OS_PLATFORM in ('linux', 'windows', 'osx') then 'desktop'
        when a.OS_PLATFORM is null then ''
        else 'other' end as platform,
        a.MARKETING_CAMPAIGN_ID,
        sa.campaign_start_date,
        sa.campaign_end_date,
        a.item_id,
        a.item_name,
        a.item_type,
        a.action_type,
        a.action_sub_type,
        a.shelf_type,
        a.shelf_imps,
        a.shelf_clicks

FROM &{pipeline_schema}.article_interactions a
LEFT JOIN &{pipeline_schema_sf}.sponsored_articles sa
        on sa.space = a.item_id and a.LOCALE = sa.LOCALE
LEFT JOIN &{pipeline_schema_sf}.article_title t
        on t.article_id = a.item_id
LEFT JOIN (
    select sales_order_id,  max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from
          (select * from operative_data where '2023-12-01' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
                   union all
           select * from pio_data where '2023-12-01' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date))
    GROUP BY 1
    ) op on op.sales_order_id = a.order_id
--left join on pio number 4 digit
LEFT JOIN (select sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    FROM pio
    GROUP BY 1,2) pio
        on pio.sales_order_id = a.order_id
--left join on pio number 4 digit
LEFT JOIN (select campaign_pio_id,sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    FROM pio GROUP BY 1,2,3) pio2 ON pio2.campaign_pio_id = a.order_id
where a.ds between '&{start_dt}' and '&{end_dt}'
;













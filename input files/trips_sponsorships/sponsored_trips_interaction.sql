
begin;
delete from &{pipeline_schema}.sponsored_trips_interaction
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema}.sponsored_trips_interaction
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
    ),   operative_data as (
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
    )


select s.DS           as ds,
        case when OS_PLATFORM like '%app%' then 'app'
           when OS_PLATFORM in ('%tablet%', '%ipad%') then 'tablet'
           when OS_PLATFORM in ('iphone_browser', 'android_browser') then 'mobile web'
           when OS_PLATFORM in ('linux', 'windows', 'osx') then 'desktop'
           when OS_PLATFORM is null then ''
           else 'other' end   as os_type,
       s.LOCALE       as locale,
       cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int) as trip_id,
       trips.TRIP_TITLE        as title,
       trips.USERNAME     as username,
       null as USER_COUNTRY_ID,
       uu.user_country_name as user_country_name,
       COALESCE(op.op_advertiser_id,pio.op_advertiser_id,pio2.op_advertiser_id)                                          as op_advertiser_id,
       COALESCE(op.advertiser_name,pio.advertiser_name,pio2.advertiser_name)                                             as advertiser_name,
       COALESCE(op.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,pio2.op1_sales_order_id,pio2.sales_order_id) as sales_order_id, -- display 4 digit number first, if null use pio op1 id
       COALESCE(op.sales_order_name,pio.sales_order_name,pio2.sales_order_name)                                          as sales_order_name,
       COALESCE(op.industry,pio.industry,pio2.industry)                                                                  as industry,
       COALESCE(op.REGION,pio.REGION,pio2.REGION)                                                                        as REGION,
       COALESCE(op.account_exec,pio.account_exec,pio2.account_exec)                                                      as account_exec,
       sum(case when s.ITEM_TYPE = 'TripContent' and s.ITEM_NAME = 'PublicTripMapZoomInOut' then 1 else 0 end)   as trip_mapzoom,
       sum(case when s.ITEM_TYPE = 'TripContent' and s.ITEM_NAME = 'PublicTripSeeMore' then 1 else 0 end)   as trip_readmore,
       sum(case when s.ITEM_TYPE = 'TripContent' and s.ITEM_NAME = 'PublicTripShare' then 1 else 0 end)    as trip_share,
       sum(case when s.ITEM_TYPE = 'TripContent' and s.ITEM_NAME = 'PublicTripSaveItems' then 1 else 0 end)    as trip_save,
       sum(case when s.ITEM_TYPE = 'TripContent' and s.ITEM_NAME = 'SponsoredContentType' then 1 else 0 end)    as trip_sponsor_click,
       count(distinct(case when s.ITEM_TYPE = 'TripContent'  and s.ITEM_NAME in ('PublicTripMapZoomInOut','PublicTripSeeMore' ,'PublicTripShare','PublicTripSaveItems','SponsoredContentType') then s.unique_id else null end) )  as uniques_with_interactions

from USER_TRACKING.public.USER_INTERACTIONS s
--left join &{pipeline_schema}.sponsored_trips st on s.trip_id = st.trip_id
JOIN &{pipeline_schema}.active_sponsored_trips_detail trips on   cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int) = trips.trip_id
left join  (select  trip_id, max(order_id) as order_id from &{pipeline_schema}.trips_orderid_mapping group by 1) map on  cast(trim(CUSTOM_DATA_JSON:tripId, '"') as int) = map.TRIP_ID
left join (select unique_id,  max(USER_IP_COUNTRY_NAME) as USER_COUNTRY_NAME from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds  between  '&{start_dt}' and '&{end_dt}' AND IS_BLESSED = 1
          group by 1 ) uu on uu.unique_id = s.UNIQUE_ID
left join (
    select sales_order_id,  max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from
  (select * from operative_data where '2023-12-01' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
           union all
   select * from pio_data where '2023-12-01' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date))
    Group by 1
    ) op on op.sales_order_id = map.order_id
--left join on pio number 4 digit
left join (select sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from pio
    group by 1,2) pio
        on pio.sales_order_id = map.order_id
--left join on pio number 4 digit
left join (select campaign_pio_id,sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from pio
    group by 1,2,3) pio2
        on pio2.campaign_pio_id = map.order_id

where ds between '&{start_dt}' and '&{end_dt}' and s.page in ('Trips','TripDetails') and s.ITEM_TYPE = 'TripContent'
group by all
;

commit;


-------------------------------------------------------------
-- remake of the Woodsy Job vw_visa_poi_actions_saves
-- aggregates all the poi (detail id) actions for each trip
-- by unioning the tables sponsored_trips_poi_actions and
-- sponsored_trips_poi_saves_wp
-- this is a final table (i.e. to be used in tableau)
-------------------------------------------------------------

begin;
delete from &{pipeline_schema}.sponsored_trips_poi
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.sponsored_trips_poi
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


select * from (
       select a.ds,
            a.os_type,
            a.locale,
            a.tripid,
            a.trip_title,
            a.detailid,
            a.detailid_name,
            a.user_id,
            a.username,
            sum(a.clicks)      as clicks,
            sum(a.likes)       as likes,
            sum(a.see_more)    as see_more,
            sum(a.shares)      as shares,
            sum(a.maps)        as maps,
            null               as saves,
            COALESCE(op.op_advertiser_id,pio.op_advertiser_id,pio2.op_advertiser_id)                                          as op_advertiser_id,
            COALESCE(op.advertiser_name,pio.advertiser_name,pio2.advertiser_name)                                             as advertiser_name,
            COALESCE(op.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,pio2.op1_sales_order_id,pio2.sales_order_id) as sales_order_id, -- display 4 digit number first, if null use pio op1 id
            COALESCE(op.sales_order_name,pio.sales_order_name,pio2.sales_order_name)                                          as sales_order_name,
            COALESCE(op.industry,pio.industry,pio2.industry)                                                                  as industry,
            COALESCE(op.REGION,pio.REGION,pio2.REGION)                                                                        as REGION,
            COALESCE(op.account_exec,pio.account_exec,pio2.account_exec)                                                      as account_exec,
            a.USER_COUNTRY_NAME,
            marketing_campaign_id,
            count(distinct(case when clicks >0 or likes>0 or see_more>0 or shares>0 or maps>0 then a.unique_id else null end) )  as uniques_with_poi_actions,
            null as uniques_with_poi_saves
      from &{pipeline_schema}.sponsored_trips_poi_actions a
      --left join &{pipeline_schema}.sponsored_trips st on a.tripid = st.trip_id
      left join  (select  trip_id, max(order_id) as order_id from &{pipeline_schema}.trips_orderid_mapping group by 1) map on a.TRIPID = map.TRIP_ID
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
      where ds between '&{start_dt}' and '&{end_dt}'
      group by 1,2,3,4,5,6,7,8,9,16,17,18,19,20,21,22,23,24)

union all

select * from (
      select s.ds,
             s.os_type,
             s.locale,
             s.tripid,
             s.trip_title,
             s.detailid,
             s.detailid_name,
             s.user_id,
             s.username,
             null       as clicks,
             null       as likes,
             null       as see_more,
             null       as shares,
             null       as maps,
             sum(s.saves) as saves,
            COALESCE(op.op_advertiser_id,pio.op_advertiser_id,pio2.op_advertiser_id)                                          as op_advertiser_id,
            COALESCE(op.advertiser_name,pio.advertiser_name,pio2.advertiser_name)                                             as advertiser_name,
            COALESCE(op.sales_order_id,pio.op1_sales_order_id,pio.sales_order_id,pio2.op1_sales_order_id,pio2.sales_order_id) as sales_order_id, -- display 4 digit number first, if null use pio op1 id
            COALESCE(op.sales_order_name,pio.sales_order_name,pio2.sales_order_name)                                          as sales_order_name,
            COALESCE(op.industry,pio.industry,pio2.industry)                                                                  as industry,
            COALESCE(op.REGION,pio.REGION,pio2.REGION)                                                                        as REGION,
            COALESCE(op.account_exec,pio.account_exec,pio2.account_exec)                                                      as account_exec,
            s.USER_COUNTRY_NAME,
            marketing_campaign_id,
            null as uniques_with_poi_actions,
            count(distinct(case when s.saves >0 then s.unique_id else null end) )  as uniques_with_poi_saves
      from &{pipeline_schema}.sponsored_trips_poi_saves_wp s
      --left join &{pipeline_schema}.sponsored_trips st on a.tripid = st.trip_id
      left join  (select  trip_id, max(order_id) as order_id from &{pipeline_schema}.trips_orderid_mapping group by 1) map on s.TRIPID = map.TRIP_ID
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
      where ds between '&{start_dt}' and '&{end_dt}'
      group by 1,2,3,4,5,6,7,8,9,16,17,18,19,20,21,22,23,24)

;

commit;

------------------------------------------------------------------
-- pulling country tied to advertiser name from operative one
-- for benchmarking
------------------------------------------------------------------

begin;

delete from &{pipeline_schema_sf}.op1_advertiser_country;

insert into &{pipeline_schema_sf}.op1_advertiser_country
WITH op1_details as (
                   select distinct   ac.ds,
                                     ac.account_id  as op_advertiser_id,
                                     ac.account_name as advertiser_name,
                                     psa.advertiser_id as advertiser_ad_server_id,
                                     ac.industry,
                                     case when ac.country in ('USA', 'US', 'United States of America') then 'United States'
                                         when ac.country = 'UAE' then 'United Arab Emirates'
                                         when ac.country in ('UK', 'England') then 'United Kingdom'
                                         when ac.country = 'FR' then 'France'
                                         when ac.country = 'SG' then 'Singapore'
                                         when ac.country = 'MX' then 'Mexico'
                                         when ac.country = 'CN' then 'China'
                                         when ac.country = 'CA' then 'Canada'
                                         when ac.country = 'BE' then 'Belgium'
                                         when ac.country = 'TT' then 'Trinidad and Tobago'
                                         when ac.country = 'JP' then 'Japan'
                                         when ac.country = 'TH' then 'Thailand'
                                         when ac.country = 'TR' then 'Turkey'
                                         when ac.country = 'ES' then 'Spain'
                                         when ac.country = 'NL' then 'Netherlands'
                                         when ac.country = 'Korea' then 'South Korea'
                                         when ac.country = 'Antigua' then 'Antigua and Barbuda'
                                         when ac.country = 'Phillipines' then 'Philippines'
                                         when ac.country = 'Panamá' then 'Panama'
                                         when ac.country = 'Barbados, WI' then 'Barbados'
                                         when ac.country = '.' then NULL
                                         else ac.country end as country_name,
                                     ctr.country,
                                     ctr.region,
                                     so.sales_order_id,
                                     soli.sales_order_line_item_id
                   from display_ads.operative_one.sales_order_line_items soli
                   left join display_ads.operative_one.production_line_item pli on pli.sales_order_line_item_id = soli.sales_order_line_item_id
                                                                                                 and soli.ds = pli.ds
                   join display_ads.operative_one.publisher_system_ad_prod_line_item_map psapli on pli.production_line_item_id = psapli.production_line_item_id
                                                                                                 and psapli.ds = pli.ds -- drops the non-productionalized line items (i.e. not pushed  to ad server)
                   left join display_ads.operative_one.publisher_system_ad psa on psapli.ad_id = psa.ad_id
                                                                                                 and psapli.ds = psa.ds
                   left join display_ads.operative_one.sales_order so on soli.sales_order_id = so.sales_order_id
                                                                                                 and soli.ds = so.ds
                   left join display_ads.operative_one.accounts ac on ac.account_id = so.advertiser_id
                                                                                                 and ac.ds = so.ds
                   left join rio_sf.anm.country_to_region ctr on lower(ctr.country) = lower(case when ac.country in ('USA', 'US', 'United States of America') then 'United States'
                                                                                                 when ac.country = 'UAE' then 'United Arab Emirates'
                                                                                                 when ac.country in ('UK', 'England') then 'United Kingdom'
                                                                                                 when ac.country = 'FR' then 'France'
                                                                                                 when ac.country = 'SG' then 'Singapore'
                                                                                                 when ac.country = 'MX' then 'Mexico'
                                                                                                 when ac.country = 'CN' then 'China'
                                                                                                 when ac.country = 'CA' then 'Canada'
                                                                                                 when ac.country = 'BE' then 'Belgium'
                                                                                                 when ac.country = 'TT' then 'Trinidad and Tobago'
                                                                                                 when ac.country = 'JP' then 'Japan'
                                                                                                 when ac.country = 'TH' then 'Thailand'
                                                                                                 when ac.country = 'TR' then 'Turkey'
                                                                                                 when ac.country = 'ES' then 'Spain'
                                                                                                 when ac.country = 'NL' then 'Netherlands'
                                                                                                 when ac.country = 'Korea' then 'South Korea'
                                                                                                 when ac.country = 'Antigua' then 'Antigua and Barbuda'
                                                                                                 when ac.country = 'Phillipines' then 'Philippines'
                                                                                                 when ac.country = 'Panamá' then 'Panama'
                                                                                                 when ac.country = 'Barbados, WI' then 'Barbados'
                                                                                                 when ac.country = '.' then NULL
                                                                                                 else ac.country end)
                   WHERE ac.ds = (select max(ds) as ds from display_ads.operative_one.accounts)
            ),

pio_adv as (
            SELECT DISTINCT advertiser_pio_id,
                            advertiser_name,
                            country,
                            industry,
                            region
            FROM (
            select  distinct pio.advertiser_pio_id,
                             CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                                  THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                                  ELSE pio.advertiser_name END AS advertiser_name,
                             pio.advertiser_ad_server_id,
                             op3.country,
                             op3.industry,
                             op3.region,
                             ROW_NUMBER() OVER (PARTITION BY pio.advertiser_pio_id ORDER BY op3.industry,op3.region) as rn
            from display_ads.pio.pio_op1_data_shim pio
            INNER JOIN op1_details op3
            ON pio.advertiser_ad_server_id = op3.advertiser_ad_server_id
                                                    AND op3.advertiser_name = (CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                                                                                    THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                                                                                    ELSE pio.advertiser_name END)
            where pio.ds = (select max(ds) from display_ads.pio.pio_op1_data_shim)
            AND pio.advertiser_pio_id IS NOT NULL
            AND pio.advertiser_ad_server_id
            and pio.line_item_delivery_status != 'not_pushed'
            and pio.organization_id_advertiser != 'Cruise Critic')
    )

select distinct  COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id)                                                         as op_advertiser_id,
                COALESCE(op1.advertiser_name,op2.advertiser_name, CASE WHEN pio.advertiser_name like 'TA_%' or pio.advertiser_name like 'CC%'
                  THEN replace(substr(pio.advertiser_name, 4, 100), '_', ' ')
                  ELSE pio.advertiser_name END)                                                                                                       as advertiser_name,
                COALESCE(op1.country, op2.country,pio_adv.country, case when pio.country in ('USA', 'US', 'United States of America') then 'United States'
                                                        when pio.country = 'UAE' then 'United Arab Emirates'
                                                        when pio.country in ('UK', 'England') then 'United Kingdom'
                                                        when pio.country = 'FR' then 'France'
                                                        when pio.country = 'SG' then 'Singapore'
                                                        when pio.country = 'MX' then 'Mexico'
                                                        when pio.country = 'CN' then 'China'
                                                        when pio.country = 'CA' then 'Canada'
                                                        when pio.country = 'BE' then 'Belgium'
                                                        when pio.country = 'TT' then 'Trinidad and Tobago'
                                                        when pio.country = 'JP' then 'Japan'
                                                        when pio.country = 'TH' then 'Thailand'
                                                        when pio.country = 'TR' then 'Turkey'
                                                        when pio.country = 'ES' then 'Spain'
                                                        when pio.country = 'NL' then 'Netherlands'
                                                        when pio.country = 'Korea' then 'South Korea'
                                                        when pio.country = 'Antigua' then 'Antigua and Barbuda'
                                                        when pio.country = 'Phillipines' then 'Philippines'
                                                        when pio.country = 'Panamá' then 'Panama'
                                                        when pio.country = 'Barbados, WI' then 'Barbados'
                                                        when pio.country = '.' then NULL
                                                        else pio.country end)                                                                             as country_name,
                COALESCE(op1.country, op2.country, pio_adv.country, ctr.country)                                                                          as country,
                COALESCE(op1.region, op2.region, pio_adv.region, ctr.region)                                                                              as region,
                COALESCE(op1.industry, op2.industry, pio_adv.industry, pio.industry)                                                                      as industry
        from display_ads.pio.PIO_OP1_DATA_SHIM pio
        LEFT JOIN display_ads.sales.pio_to_op1_order_id_mapping pto ON cast(pio.line_item_pio_id as int) = cast(pto.pio_line_item_id as int)
        LEFT JOIN op1_details as op1 ON pto.op1_sales_order_line_item_id = op1.sales_order_line_item_id
        LEFT JOIN ( SELECT distinct campaign_number,
                                    campaign_pio_id
                    FROM display_ads.pio.PIO_OP1_DATA_SHIM
                    WHERE campaign_number is not null) pio2 ON pio.campaign_pio_id = pio2.campaign_pio_id
        LEFT JOIN ( SELECT distinct campaign_pio_id,
                                    op1_sales_order_id
                    FROM  display_ads.sales.pio_to_op1_order_id_mapping) op ON pio.campaign_pio_id = op.campaign_pio_id
        LEFT JOIN (SELECT distinct op_advertiser_id,
                                   advertiser_name,
                                   industry,
                                   country_name,
                                   country,
                                   region,
                                   sales_order_id
                     FROM op1_details
                    WHERE sales_order_id IS NOT NULL) op2 ON op.op1_sales_order_id = op2.sales_order_id
        LEFT JOIN pio_adv ON pio.advertiser_pio_id = pio_adv.advertiser_pio_id
        left join rio_sf.anm.country_to_region ctr on lower(ctr.country) = lower(case when pio.country in ('USA', 'US', 'United States of America') then 'United States'
                                                                                                                         when pio.country = 'UAE' then 'United Arab Emirates'
                                                                                                                         when pio.country in ('UK', 'England') then 'United Kingdom'
                                                                                                                         when pio.country = 'FR' then 'France'
                                                                                                                         when pio.country = 'SG' then 'Singapore'
                                                                                                                         when pio.country = 'MX' then 'Mexico'
                                                                                                                         when pio.country = 'CN' then 'China'
                                                                                                                         when pio.country = 'CA' then 'Canada'
                                                                                                                         when pio.country = 'BE' then 'Belgium'
                                                                                                                         when pio.country = 'TT' then 'Trinidad and Tobago'
                                                                                                                         when pio.country = 'JP' then 'Japan'
                                                                                                                         when pio.country = 'TH' then 'Thailand'
                                                                                                                         when pio.country = 'TR' then 'Turkey'
                                                                                                                         when pio.country = 'ES' then 'Spain'
                                                                                                                         when pio.country = 'NL' then 'Netherlands'
                                                                                                                         when pio.country = 'Korea' then 'South Korea'
                                                                                                                         when pio.country = 'Antigua' then 'Antigua and Barbuda'
                                                                                                                         when pio.country = 'Phillipines' then 'Philippines'
                                                                                                                         when pio.country = 'Panamá' then 'Panama'
                                                                                                                         when pio.country = 'Barbados, WI' then 'Barbados'
                                                                                                                         when pio.country = '.' then NULL
                                                                                                                         else pio.country end)
        where'&{start_dt}' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
        and pio.line_item_delivery_status != 'not_pushed'
        and pio.organization_id_advertiser != 'Cruise Critic'
        and pio.ds = (select max(ds) as ds from display_ads.pio.pio_op1_data_shim)
        and COALESCE(op1.country, op2.country,pio_adv.country, case when pio.country in ('USA', 'US', 'United States of America') then 'United States'
                                                        when pio.country = 'UAE' then 'United Arab Emirates'
                                                        when pio.country in ('UK', 'England') then 'United Kingdom'
                                                        when pio.country = 'FR' then 'France'
                                                        when pio.country = 'SG' then 'Singapore'
                                                        when pio.country = 'MX' then 'Mexico'
                                                        when pio.country = 'CN' then 'China'
                                                        when pio.country = 'CA' then 'Canada'
                                                        when pio.country = 'BE' then 'Belgium'
                                                        when pio.country = 'TT' then 'Trinidad and Tobago'
                                                        when pio.country = 'JP' then 'Japan'
                                                        when pio.country = 'TH' then 'Thailand'
                                                        when pio.country = 'TR' then 'Turkey'
                                                        when pio.country = 'ES' then 'Spain'
                                                        when pio.country = 'NL' then 'Netherlands'
                                                        when pio.country = 'Korea' then 'South Korea'
                                                        when pio.country = 'Antigua' then 'Antigua and Barbuda'
                                                        when pio.country = 'Phillipines' then 'Philippines'
                                                        when pio.country = 'Panamá' then 'Panama'
                                                        when pio.country = 'Barbados, WI' then 'Barbados'
                                                        when pio.country = '.' then NULL
                                                        else pio.country end) is NOT NULL
        and COALESCE(op1.industry, op2.industry, pio_adv.industry, pio.industry) IS NOT NULL
        and  COALESCE(op1.country, op2.country, pio_adv.country, ctr.country) IS NOT NULL
        and  CONCAT(COALESCE(op1.op_advertiser_id,op2.op_advertiser_id,pio.advertiser_pio_id),COALESCE(op1.industry, op2.industry, pio_adv.industry, pio.industry) ) <> '1456722Agency' --manual dedup

        union all

        select distinct op_advertiser_id,
                        advertiser_name,
                        country_name,
                        country,
                        region,
                        industry
        FROM op1_details
        WHERE
             '&{start_dt}' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
;

commit;
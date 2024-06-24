
BEGIN;
DELETE FROM &{pipeline_schema_sf}.article_enhance_inter
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.article_enhance_inter
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

    articles_reporting as(

        select imp.ds,
               imp.LOCALE,
               imp.OS_PLATFORM,
               imp.COMMERCE_COUNTRY_ID,
               imp.USER_COUNTRY_NAME,
               imp.ITEM_ID,
               imp.ITEM_NAME,
               imp.ITEM_TYPE,
               null as  action_type,
               null as action_sub_type,
               case when imp.order_id is null then h.sales_order_id
                    else imp.order_id
               end  as order_id,
               imp.url_id,
               imp.utm_source,
               imp.MARKETING_CAMPAIGN_ID,
               count(*) as total_impression,
               count(distinct imp.UNIQUE_ID) as total_users,
               null as total_interaction
        from  &{pipeline_schema_sf}.article_lookback_pageview imp
             --left join RIO_SF.rust.a_unique_users u on imp.UNIQUE_ID = u.UNIQUE_ID and imp.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
             left join display_ads.sales.historical_articles_order_mapping h on imp.url_id = h.url_article_id
        where  imp.ds between '&{start_dt}' and '&{end_dt}'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

        union all

        select r.ds,
           case
           when (r.locale =  'ar_EG') then 'ar-EG-u-nu-latn'
           when (r.locale =  'ar') then 'ar-US-u-nu-latn'
           when (r.locale =  'cs') then 'cs'
           when (r.locale =  'da') then 'da-DK'
           when (r.locale =  'de_AT') then 'de-AT'
           when (r.locale =  'de_BE') then 'de-BE'
           when (r.locale =  'de_CH') then 'de-CH'
           when (r.locale =  'de') then 'de-DE'
           when (r.locale =  'el') then 'el-GR'
           when (r.locale =  'en_AU') then 'en-AU'
           when (r.locale =  'en_CA') then 'en-CA'
           when (r.locale =  'en_UK') then 'en-GB'
           when (r.locale =  'en_HK') then 'en-HK'
           when (r.locale =  'en_IE') then 'en-IE'
           when (r.locale =  'en_IN') then 'en-IN'
           when (r.locale =  'en_MY') then 'en-MY'
           when (r.locale =  'en_NZ') then 'en-NZ'
           when (r.locale =  'en_PH') then 'en-PH'
           when (r.locale =  'en_SG') then 'en-SG'
           when (r.locale =  'en_US') then 'en-US'
           when (r.locale =  'en_ZA') then 'en-ZA'
           when (r.locale =  'es_AR') then 'es-AR'
           when (r.locale =  'es_CL') then 'es-CL'
           when (r.locale =  'es_CO') then 'es-CO'
           when (r.locale =  'es') then 'es-ES'
           when (r.locale =  'es_MX') then 'es-MX'
           when (r.locale =  'es_PE') then 'es-PE'
           when (r.locale =  'es_VE') then 'es-VE'
           when (r.locale =  'fr_BE') then 'fr-BE'
           when (r.locale =  'fr_CA') then 'fr-CA'
           when (r.locale =  'fr_CH') then 'fr-CH'
           when (r.locale =  'fr') then 'fr-FR'
           when (r.locale =  'he_IL') then 'he-IL'
           when (r.locale =  'hu') then 'hu'
           when (r.locale =  'id') then 'id-ID'
           when (r.locale =  'it_CH') then 'it-CH'
           when (r.locale =  'it') then 'it-IT'
           when (r.locale =  'iw') then 'iw'
           when (r.locale =  'ja') then 'ja-JP'
           when (r.locale =  'ko') then 'ko-KR'
           when (r.locale =  'no') then 'nb-NO'
           when (r.locale =  'nl_BE') then 'nl-BE'
           when (r.locale =  'nl') then 'nl-NL'
           when (r.locale =  'pl') then 'pl'
           when (r.locale =  'pt') then 'pt-BR'
           when (r.locale =  'pt_PT') then 'pt-PT'
           when (r.locale =  'ru') then 'ru-RU'
           when (r.locale =  'sv') then 'sv-SE'
           when (r.locale =  'th') then 'th-u-ca-gregory'
           when (r.locale =  'tr') then 'tr-TR'
           when (r.locale =  'vi') then 'vi-VN'
           when (r.locale =  'zh_CN') then 'zh-CN'
           when (r.locale =  'zh') then 'zh-Hans-US'
           when (r.locale =  'zh_HK') then 'zh-Hant-HK'
           when (r.locale =  'zh_TW') then 'zh-Hant-TW'
           else r.locale
           end as locale,
               r.OS_PLATFORM,
               u.COMMERCE_COUNTRY_ID,
               u.USER_COUNTRY_NAME,
               r.ITEM_ID,
               r.ITEM_NAME,
               r.ITEM_TYPE,
               r. action_type,
               r.action_sub_type,
               case when map.order_id is null then h.sales_order_id
               else map.order_id end as order_id,
               concat('l',r.item_id) as url_id,
               null as utm_source,
               MCID as MARKETING_CAMPAIGN_ID,
               --regexp_substr(utm.url,'source\\W+\\w+\\D\\w+') as utm_source,
               --regexp_substr(utm.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
               null as total_impression,
               null as total_users,
               count(distinct r.INTERACTION_ID) as total_interaction
        from user_tracking.public.USER_INTERACTIONS r
            left join ( select distinct URL_ID, ORDER_ID from &{pipeline_schema_sf}.article_order_unique_id_mapping ) map
            on URL_ID = concat('l',r.item_id)
            left join RIO_SF.rust.a_unique_users u on r.UNIQUE_ID = u.UNIQUE_ID and r.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
            left join display_ads.sales.historical_articles_order_mapping h on concat('l',r.item_id) = h.url_article_id
        where  r.item_type in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink')
        and r.ds between '&{start_dt}' and '&{end_dt}'
        --and page = 'Articles'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

        union all

        select r.ds,
           case
           when (r.locale =  'ar_EG') then 'ar-EG-u-nu-latn'
           when (r.locale =  'ar') then 'ar-US-u-nu-latn'
           when (r.locale =  'cs') then 'cs'
           when (r.locale =  'da') then 'da-DK'
           when (r.locale =  'de_AT') then 'de-AT'
           when (r.locale =  'de_BE') then 'de-BE'
           when (r.locale =  'de_CH') then 'de-CH'
           when (r.locale =  'de') then 'de-DE'
           when (r.locale =  'el') then 'el-GR'
           when (r.locale =  'en_AU') then 'en-AU'
           when (r.locale =  'en_CA') then 'en-CA'
           when (r.locale =  'en_UK') then 'en-GB'
           when (r.locale =  'en_HK') then 'en-HK'
           when (r.locale =  'en_IE') then 'en-IE'
           when (r.locale =  'en_IN') then 'en-IN'
           when (r.locale =  'en_MY') then 'en-MY'
           when (r.locale =  'en_NZ') then 'en-NZ'
           when (r.locale =  'en_PH') then 'en-PH'
           when (r.locale =  'en_SG') then 'en-SG'
           when (r.locale =  'en_US') then 'en-US'
           when (r.locale =  'en_ZA') then 'en-ZA'
           when (r.locale =  'es_AR') then 'es-AR'
           when (r.locale =  'es_CL') then 'es-CL'
           when (r.locale =  'es_CO') then 'es-CO'
           when (r.locale =  'es') then 'es-ES'
           when (r.locale =  'es_MX') then 'es-MX'
           when (r.locale =  'es_PE') then 'es-PE'
           when (r.locale =  'es_VE') then 'es-VE'
           when (r.locale =  'fr_BE') then 'fr-BE'
           when (r.locale =  'fr_CA') then 'fr-CA'
           when (r.locale =  'fr_CH') then 'fr-CH'
           when (r.locale =  'fr') then 'fr-FR'
           when (r.locale =  'he_IL') then 'he-IL'
           when (r.locale =  'hu') then 'hu'
           when (r.locale =  'id') then 'id-ID'
           when (r.locale =  'it_CH') then 'it-CH'
           when (r.locale =  'it') then 'it-IT'
           when (r.locale =  'iw') then 'iw'
           when (r.locale =  'ja') then 'ja-JP'
           when (r.locale =  'ko') then 'ko-KR'
           when (r.locale =  'no') then 'nb-NO'
           when (r.locale =  'nl_BE') then 'nl-BE'
           when (r.locale =  'nl') then 'nl-NL'
           when (r.locale =  'pl') then 'pl'
           when (r.locale =  'pt') then 'pt-BR'
           when (r.locale =  'pt_PT') then 'pt-PT'
           when (r.locale =  'ru') then 'ru-RU'
           when (r.locale =  'sv') then 'sv-SE'
           when (r.locale =  'th') then 'th-u-ca-gregory'
           when (r.locale =  'tr') then 'tr-TR'
           when (r.locale =  'vi') then 'vi-VN'
           when (r.locale =  'zh_CN') then 'zh-CN'
           when (r.locale =  'zh') then 'zh-Hans-US'
           when (r.locale =  'zh_HK') then 'zh-Hant-HK'
           when (r.locale =  'zh_TW') then 'zh-Hant-TW'
           else r.locale
           end as locale,
        r.OS_PLATFORM,
        u.COMMERCE_COUNTRY_ID,
        u.USER_COUNTRY_NAME,
        split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3) as ITEM_ID,
        r.ITEM_NAME,
        r.ITEM_TYPE,
        r. action_type,
        r.action_sub_type,
        case when map.order_id is null then h.sales_order_id
        else map.order_id end as order_id,
        concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3)) as url_id,
        null as utm_source,
        MCID as MARKETING_CAMPAIGN_ID,
        --regexp_substr(utm.url,'source\\W+\\w+\\D\\w+') as utm_source,
        --regexp_substr(utm.url,'medium\\W+\\w+\\D\\w+') as utm_medium,
        null as total_impression,
        null as total_users,
        count(distinct r.INTERACTION_ID) as total_interaction
        from user_tracking.public.USER_INTERACTIONS r
            left join ( select distinct URL_ID, ORDER_ID from &{pipeline_schema_sf}.article_order_unique_id_mapping ) map
            on URL_ID = concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3))
            left join RIO_SF.rust.a_unique_users u on r.UNIQUE_ID = u.UNIQUE_ID and r.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
            left join display_ads.sales.historical_articles_order_mapping h on concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3)) = h.url_article_id
        where  r.team = 'TAPS' and r.page = 'Articles'
             and r.item_name in ('ReefFeedScroll','ReefFeedClick')
             --and r.item_type in ('Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
             and r.ds between '&{start_dt}' and '&{end_dt}'
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
       a.total_impression,
       a.total_users,
       a.total_interaction
from articles_reporting a
left join &{pipeline_schema_sf}.sponsored_articles sa on sa.space = a.item_id and a.LOCALE = sa.LOCALE
left join &{pipeline_schema_sf}.article_title t on t.article_id = a.item_id
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
--left join on pio number 4 digit
left join (select campaign_pio_id,sales_order_id, op1_sales_order_id, max(op_advertiser_id) as op_advertiser_id , max(advertiser_name) as advertiser_name,
    max(sales_order_name) as sales_order_name, max(industry) as industry, max(REGION) as REGION, max(account_exec) as account_exec
    from pio
    group by 1,2,3) pio2
        on pio2.campaign_pio_id = a.order_id
where a.ds between '&{start_dt}' and '&{end_dt}' and a.item_id is not null
;

commit;

DELETE FROM &{pipeline_schema}.article_enhance_inter WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';


------------------------------------------------ 1 ------------------------------------------------





with articles_reporting as
 -- INTERACTION
(
             SELECT DS,
                    LOCALE,
                    OS_PLATFORM,
                    U.COMMERCE_COUNTRY_ID,
                    U.USER_COUNTRY_NAME,
                    CASE
                        WHEN r.ITEM_TYPE IN ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink') THEN r.ITEM_ID
                        WHEN r.team = 'TAPS' and r.page = 'Articles' and r.item_name in ('ReefFeedScroll','ReefFeedClick') THEN split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3)
                    END AS ITEM_ID,
                    ITEM_NAME,
                    ITEM_TYPE,
                    ACTION_TYPE,
                    ACTION_SUB_TYPE,
                    CASE
                        WHEN map.ORDER_ID IS NULL THEN h.SALES_ORDER_ID
                        ELSE map.ORDER_ID
                    END AS ORDER_ID,
                    CASE
                        WHEN r.team = 'TAPS' and r.page = 'Articles' and r.item_name IN ('ReefFeedScroll','ReefFeedClick')
                            THEN CAST(SPLIT_PART(REGEXP_SUBSTR(R.CUSTOM_DATA, 'Articles-l\\w*-'), '-', 2) AS STRING)
                        WHEN r.item_type IN ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink') THEN concat('l',r.item_id)
                    END AS URL_ID,
                    NULL                                                                                AS UTM_SOURCE,
                    MARKETING_CAMPAIGN_ID,
                    NULL                                                                                AS TOTAL_IMPRESSION,
                    NULL                                                                                AS TOTAL_USERS,
                    TOTAL_INTERACTION

                    FROM &{pipeline_schema}.base_media_interactions r
                    LEFT JOIN ( select distinct URL_ID, ORDER_ID from &{pipeline_schema_sf}.article_order_unique_id_mapping ) map
                        ON r.URL_ID = concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3))
                    LEFT JOIN &{pipeline_schema}.base_unique_users u
                        ON r.UNIQUE_ID = u.UNIQUE_ID AND r.ds = u.ds AND u.ds BETWEEN '&{start_dt}' AND '&{end_dt}'
                    LEFT JOIN display_ads.sales.historical_articles_order_mapping h
                        ON cast(split_part(regexp_substr(r.custom_data,'Articles-l\\w*-'),'-',2) AS STRING) = h.url_article_id
                    WHERE r.ITEM_TYPE IN ('articleSponsorInfoImpression', 'articleLinkClick')
                    AND r.DS BETWEEN '&{start_dt}' AND '&{end_dt}'

UNION ALL

-- article_lookback_pageview
            SELECT
                imp.ds,
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
                left join display_ads.sales.historical_articles_order_mapping h on imp.url_id = h.url_article_id
                where  imp.ds between '&{start_dt}' and '&{end_dt}'

)

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
                sa.article_title,
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
                FROM article_reporting a
                left join &{pipeline_schema_sf}.sponsored_articles sa
                ON sa.space = a.item_id and a.LOCALE = sa.LOCALE
                left join
                (select * from operative_data where '&{start_dt}' < (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
                union all
                select * from pio_data where '&{start_dt}' >= (select max(pio_go_live_date) from display_ads.sales.pio_go_live_date)
                ) op on op.sales_order_id = a.order_id
                where a.ds between '&{start_dt}' and '&{end_dt}'

;




------------------------------------------------ 2 ------------------------------------------------
INSERT INTO &{pipeline_schema_sf}.article_enhance_inter
SELECT
    DS,
    URL_ID,
    ORDER_ID,
    UTM_SOURCE,
    LOCALE,
    USER_COUNTRY_NAME,
    MARKETING_CAMPAIGN_ID,
    ITEM_ID,
    ITEM_NAME,
    ACTION_TYPE,
    ACTION_SUB_TYPE,
    TOTAL_IMPRESSION,
    TOTAL_USERS,
    TOTAL_INTERACTION

FROM &{pipeline_schema_sf}.article_reporting
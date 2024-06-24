DELETE FROM &{pipeline_schema}.article_interactions WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';

CREATE OR REPLACE TABLE &{pipeline_schema}.article_interactions AS
    SELECT
                    DS,
                    LOCALE,
                    OS_PLATFORM,
                    U.COMMERCE_COUNTRY_ID,
                    U.USER_COUNTRY_NAME,
                    CASE
                        WHEN r.ITEM_TYPE IN ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink')
                            THEN r.ITEM_ID
                        WHEN r.team = 'TAPS' and r.page = 'Articles' and r.item_name in ('ReefFeedScroll','ReefFeedClick')
                            THEN split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3)
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
                    NULL AS UTM_SOURCE,
                    MARKETING_CAMPAIGN_ID,
                    NULL AS TOTAL_IMPRESSION,
                    NULL AS TOTAL_USERS,
                    SHELF_TYPE,
                    SUM(SHELF_IMPS) AS SHELF_IMPS,
                    SUM(SHELF_CLICKS) AS SHELF_CLICKS,
                    SUM(TOTAL_INTERACTION) AS TOTAL_INTERACTION

    FROM &{pipeline_schema}.base_media_interactions r
    LEFT JOIN ( select distinct URL_ID, ORDER_ID from &{pipeline_schema_sf}.article_order_unique_id_mapping ) map
        ON r.URL_ID = concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3))
    LEFT JOIN &{pipeline_schema}.base_unique_users u
        ON r.UNIQUE_ID = u.UNIQUE_ID AND r.ds = u.ds AND u.ds BETWEEN '&{start_dt}' AND '&{end_dt}'
    LEFT JOIN display_ads.sales.historical_articles_order_mapping h
        ON cast(split_part(regexp_substr(r.custom_data,'Articles-l\\w*-'),'-',2) AS STRING) = h.url_article_id
    WHERE r.ITEM_TYPE IN ('articleSponsorInfoImpression', 'articleLinkClick')
    AND r.DS BETWEEN '&{start_dt}' AND '&{end_dt}';







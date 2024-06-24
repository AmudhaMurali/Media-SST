DELETE FROM &{pipeline_schema}.article_interactions WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';
DELETE FROM &{pipeline_schema}.article_impressions WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';


-------------------------------------------- INTERACTIONS --------------------------------------------
INSERT INTO &{pipeline_schema}.article_interactions
SELECT
    m_int.DS,
    m_int.LOCALE,
    m_int.OS_PLATFORM,
    U.COMMERCE_COUNTRY_ID,
    U.USER_COUNTRY_NAME,
    m_int.ITEM_ID,
    m_int.ITEM_NAME,
    m_int.ITEM_TYPE,
    m_int.ACTION_TYPE,
    m_int.ACTION_SUB_TYPE,
    -- ORDER ID
    CASE
        WHEN item_type in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink')
            THEN concat('l',item_id)
        WHEN team = 'TAPS' and page = 'Articles' and item_name in ('ReefFeedScroll','ReefFeedClick')
            THEN concat('l',split_part(regexp_substr(custom_data,'articleId\\W*\\w*'),'"',3))
    END AS url_id,
    NULL AS UTM_SOURCE,
    MARKETING_CAMPAIGN_ID,
    NULL AS TOTAL_IMPRESSION,
    NULL AS TOTAL_USERS,
    TOTAL_INTERACTION
FROM &{pipeline_schema}.base_media_interactions m_int
LEFT JOIN RIO_SF.rust.a_unique_users U ON r.UNIQUE_ID = u.UNIQUE_ID and r.ds = u.ds and u.ds between '&{start_dt}' and '&{end_dt}'
WHERE  (team = 'TAPS' and page = 'Articles' and item_name in ('ReefFeedScroll','ReefFeedClick'))
OR item_type in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink')
AND r.ds between '&{start_dt}' and '&{end_dt}';



-------------------------------------------- BLESSED JOINED --------------------------------------------
INSERT INTO &{pipeline_schema}.article_blessed_joined
SELECT
    DS,
    LOCALE,
    USER_COUNTRY_ID,
    USER_COUNTRY_NAME,
    COMMERCE_COUNTRY_ID,
    MARKETING_CAMPAIGN_ID,
    URL,
    UNIQUE_ID,
    OS_PLATFORM,
    ITEM_ID,
    ITEM_NAME,
    ITEM_TYPE,
    ACTION_TYPE,
    ACTION_SUB_TYPE,
    concat('l',split_part(regexp_substr(r.custom_data,'articleId\\W*\\w*'),'"',3)) as url_id,

    TOTAL_INTERACTION,
FROM &{pipeline_schema}.base_media_blessed_joined_only_pv
WHERE ds between '&{start_dt}' and '&{end_dt}';




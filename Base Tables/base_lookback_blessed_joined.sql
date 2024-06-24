DELETE FROM &{pipeline_schema}.base_media_blessed_joined_only_pv WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';
DELETE FROM &{pipeline_schema}.base_media_blessed_joined WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.base_media_blessed_joined_only_pv
SELECT
    DS,
    UNIQUE_ID,
    LOCALE,
    USER_COUNTRY_ID,
    USER_COUNTRY_NAME,
    COMMERCE_COUNTRY_ID,
    MARKETING_CAMPAIGN_ID,
    OS_TYPE_NAME AS OS_PLATFORM,
    URL,
    UNIQUE_ID
FROM RIO_SF.ANM.A_LOOKBACK_BLESSED_JOINED_ONLY_PV
GROUP BY ALL;

----------------------------------------------------------------------------------------------------

INSERT INTO &{pipeline_schema}.base_media_blessed_joined
SELECT
    SERVLET_NAME,
    LOCALE,
    URL,
    USER_COUNTRY_NAME,
    OS_TYPE_NAME,
    UNIQUE_ID,
    MARKETING_CAMPAIGN_ID

FROM USER_TRACKING.PUBLIC.A_LOOKBACK_BLESSED_JOINED;



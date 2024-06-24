DELETE FROM &{pipeline_schema}.base_media_impressions
WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';


INSERT INTO &{pipeline_schema}.base_media_impressions
SELECT
    DS,
    LOCALE,
    USER_AGENT,
    CASE
        WHEN page = 'HotelsFusion' THEN 'Hotels'
        WHEN page = 'AttractionsFusion' THEN 'Attractions'
        ELSE page
        END AS SERVLET,
    COALESCE(geo_id, 0) AS GEO_ID,
    MCID AS CAMPAIGN_ID,
    CASE
        WHEN OS_PLATFORM like '%app%' then 'app'
        WHEN OS_PLATFORM in ('%tablet%', '%ipad%') then 'tablet'
        WHEN OS_PLATFORM in ('iphone_browser', 'android_browser') then 'mobile web'
        WHEN OS_PLATFORM in ('linux', 'windows', 'osx') then 'desktop'
        WHEN OS_PLATFORM is null then ''
    ELSE 'other' END AS OS_PLATFORM,
    UNIQUE_ID,
    PAGE_UID,
    PAGE,
    ITEM_ID,
    ITEM_TYPE,
    ITEM_NAME,
    CUSTOM_DATA_JSON,
    COUNT(DISTINCT
        CASE
            WHEN 'item_name' IN ('nonPOISponsoredPins', 'nonTripadvisorLocationSponsoredPins')
		    THEN CUSTOM_DATA_JSON['mapSession']
		    END) AS map_open_ct,

	SUM(CASE
        WHEN 'item_name' IN ('nonPOISponsoredCard', 'nonTripadvisorLocationSponsoredCard')
        THEN 1
        ELSE 0
    END) AS pin_hover_tab_ct

FROM USER_TRACKING.PUBLIC.USER_IMPRESSIONS
WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}'
GROUP BY ALL;























DELETE FROM &{pipeline_schema}.data_sponsor_map_latest WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';


-- -------------------------------------------- INTERACTIONS --------------------------------------------
-- INSERT INTO &{pipeline_schema}.sponsor_map_interactions
-- SELECT
-- 			ds
-- 		, 	locale
-- 		, 	user_agent
--         ,   SERVLET
--  		, 	GEO_ID
-- 		, 	COALESCE(CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id
-- 		,	title_cta_click
-- 		,	link_cta_click
-- from &{pipeline_schema}.base_media_interactions
-- where ITEM_NAME IN ('clickCTALinkNonPOISponsoredCard', 'clickCTALinkNonTripadvisorLocationSponsoredCard', 'clickTitleLinkNonPOISponsoredCard', 'clickTitleLinkNonTripadvisorLocationSponsoredCard')
-- and ds between '&{start_dt}' and '&{end_dt}'
-- group by 1,2,3,4,5,6


-- -------------------------------------------- IMPRESSIONS --------------------------------------------
-- INSERT INTO &{pipeline_schema}.sponsor_map_impressions
-- SELECT
-- 			ds
-- 		, 	locale
-- 		, 	user_agent
-- 		, 	SERVLET
-- 		, 	GEO_ID
-- 		, 	COALESCE(CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id
-- 		,	SUM(pin_hover_tab_ct)
--         ,   map_open_ct
-- from &{pipeline_schema}.base_media_impressions
-- where ds between '&{start_dt}' and '&{end_dt}'
-- group by 1,2,3,4,5,6


-------------------------------------------- combined sponsor map --------------------------------------------

INSERT INTO &{pipeline_schema}.data_sponsor_map_latest
SELECT
    sm_imp.DS,
    sm_imp.LOCALE,
    sm_imp.USER_AGENT,
    sm_imp.SERVLET,
    sm_imp.GEO_ID,
    l.PRIMARY_NAME as GEO_NAME,
    COALESCE(sm_imp.CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id,
    ssc.CAMPAIGN_NAME,
    SUM(sm_imp.MAP_OPEN_CT) AS MAP_OPEN_CT,
    SUM(PIN_HOVER_TAB_CT) AS PIN_HOVER_TAB_CT,
	null as TITLE_CTA_CLICK,
	null as LINK_CTA_CLICK
FROM &{pipeline_schema}.base_media_impressions sm_imp
LEFT JOIN places.common.a_location_details_latest l ON (l.geo_id = sm_imp.geo_id)
LEFT JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS ssc ON (ssc.campaign_id = sm_imp.campaign_id)
WHERE ITEM_NAME IN('nonPOISponsoredCard', 'nonTripadvisorLocationSponsoredCard') -- item_name changed (https://jira.tamg.io/browse/ADS-10884)
and ds between '&{start_dt}' and '&{end_dt}'
GROUP BY 1,2,3,4,5,6

UNION ALL

SELECT
    sm_int.DS,
    sm_int.LOCALE,
    sm_int.USER_AGENT,
    sm_int.SERVLET,
    sm_int.GEO_ID,
    l.PRIMARY_NAME as GEO_NAME,
    COALESCE(sm_int.CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id ,
    ssc.CAMPAIGN_NAME,
    0 as MAP_OPEN_CT,
    null as PIN_HOVER_TAB_CT,
    SUM(sm_int.TITLE_CTA_CLICK) AS TITLE_CTA_CLICK,
    SUM(sm_int.LINK_CTA_CLICK) AS LINK_CTA_CLICK
FROM &{pipeline_schema}.base_media_interactions sm_int
LEFT JOIN places.common.a_location_details_latest l ON (l.geo_id = sm_imp.geo_id)
LEFT JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS ssc ON (ssc.campaign_id = sm_imp.campaign_id)
where ITEM_NAME IN ('clickCTALinkNonPOISponsoredCard', 'clickCTALinkNonTripadvisorLocationSponsoredCard', 'clickTitleLinkNonPOISponsoredCard', 'clickTitleLinkNonTripadvisorLocationSponsoredCard')
and ds between '&{start_dt}' and '&{end_dt}'
group by 1,2,3,4,5,6
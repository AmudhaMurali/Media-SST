begin;
delete from &{pipeline_schema}.cta_click_count
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema}.cta_click_count

SELECT
			ds
		, 	locale
		, 	user_agent
		, 	CASE
				WHEN page = 'HotelsFusion' THEN 'Hotels'
				WHEN page = 'AttractionsFusion' THEN 'Attractions'
				ELSE page
			END AS servlet
		, 	COALESCE(geo_id, 0) AS geo_id -- HotelsList only has zfp_id
		, 	COALESCE(CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id -- Initial launch data didn't include campaign related columns and there was only one campaign at that time.
		, 	count_if(ITEM_NAME IN ('clickTitleLinkNonPOISponsoredCard', 'clickTitleLinkNonTripadvisorLocationSponsoredCard')) AS title_cta_click
		, 	count_if(ITEM_NAME IN ('clickCTALinkNonPOISponsoredCard', 'clickCTALinkNonTripadvisorLocationSponsoredCard')) AS link_cta_click
	from USER_TRACKING.PUBLIC.USER_INTERACTIONS
	where ITEM_NAME IN ('clickCTALinkNonPOISponsoredCard', 'clickCTALinkNonTripadvisorLocationSponsoredCard', 'clickTitleLinkNonPOISponsoredCard', 'clickTitleLinkNonTripadvisorLocationSponsoredCard') -- item_name changed (https://jira.tamg.io/browse/ADS-10884)
	AND ds between '&{start_dt}' and '&{end_dt}'
	GROUP BY 1,2,3,4,5,6

;

commit;
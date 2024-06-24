begin;
delete from &{pipeline_schema}.pin_hover_tab_count
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema}.pin_hover_tab_count

	SELECT
			ds
		, 	locale
		,  	user_agent
		,   CASE
				WHEN page = 'HotelsFusion' THEN 'Hotels'
				WHEN page = 'AttractionsFusion' THEN 'Attractions'
				ELSE page
			END AS servlet
		, 	COALESCE(geo_id, 0) AS geo_id -- HotelsList only has zfp_id
		, 	COALESCE(CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id -- Initial launch data didn't include campaign related columns and there was only one campaign at that time.
		, 	count(*) AS pin_hover_tab_ct
	FROM USER_TRACKING.PUBLIC.USER_IMPRESSIONS
	WHERE item_name IN ('nonPOISponsoredCard', 'nonTripadvisorLocationSponsoredCard') -- item_name changed (https://jira.tamg.io/browse/ADS-10884)
	  AND ds between '&{start_dt}' and '&{end_dt}'
	GROUP BY 1,2,3,4,5,6

;

commit;
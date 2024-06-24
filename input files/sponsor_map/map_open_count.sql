begin;
delete from &{pipeline_schema}.map_open_count
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema}.map_open_count

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
		, 	COALESCE(CUSTOM_DATA_JSON['campaignId']::INT, 918) AS campaign_id
		, 	count(distinct(CUSTOM_DATA_JSON['mapSession'])) AS map_open_ct
	FROM USER_TRACKING.PUBLIC.USER_IMPRESSIONS
	WHERE item_name IN ('nonPOISponsoredPins', 'nonTripadvisorLocationSponsoredPins') -- item_name changed (https://jira.tamg.io/browse/ADS-10884)
	  and ds between '&{start_dt}' and '&{end_dt}'
	GROUP BY 1,2,3,4,5,6

;

commit;
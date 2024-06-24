begin;
delete from &{pipeline_schema}.data_sponsor_map
where ds between '&{start_dt}' and '&{end_dt}';


insert into &{pipeline_schema}.data_sponsor_map

SELECT
			phtc.ds
		, 	phtc.locale
		, 	phtc.user_agent
		, 	phtc.servlet
		, 	phtc.geo_id AS geo_id
		, 	l.primaryname AS geo_name
		, 	phtc.campaign_id
		, 	ssc.campaign_name
		, 	0 AS map_open_ct
		, 	phtc.pin_hover_tab_ct
		, 	ccc.title_cta_click
		, 	ccc.link_cta_click
	FROM &{pipeline_schema}.pin_hover_tab_count phtc
	LEFT JOIN &{pipeline_schema}.cta_click_count ccc ON (ccc.ds = phtc.ds and ccc.LOCALE = phtc.LOCALE and ccc.USER_AGENT = phtc.USER_AGENT and ccc.servlet = phtc.servlet and ccc.geo_id = phtc.geo_id and ccc.campaign_id = phtc.campaign_id AND ccc.ds BETWEEN '&{start_dt}' and '&{end_dt}')
	LEFT JOIN rio_sf.public.t_location l ON (l.id = phtc.geo_id)
	LEFT JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS ssc ON (ssc.campaign_id = phtc.campaign_id)
WHERE phtc.ds BETWEEN '&{start_dt}' and '&{end_dt}'


union all

	SELECT
			moc.ds
		, 	moc.locale
		, 	moc.user_agent
		, 	moc.servlet
		, 	moc.geo_id AS geo_id
		, 	l.primaryname AS geo_name
		, 	moc.campaign_id
		, 	ssc.campaign_name
		, 	moc.map_open_ct
		, 	null as pin_hover_tab_ct
		, 	null as title_cta_click
		, 	null as link_cta_click
	FROM &{pipeline_schema}.map_open_count moc
	LEFT JOIN rio_sf.public.t_location l ON (l.id = moc.geo_id)
	LEFT JOIN DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS ssc ON (ssc.campaign_id = moc.campaign_id)
    where moc.ds BETWEEN '&{start_dt}' and '&{end_dt}'




;

commit;
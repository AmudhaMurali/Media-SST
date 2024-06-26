--- this joins the mei_all_competitive_geo set from the
--- machine learning output and outputs the top 10 competitive
--- geos per each ad geo AND joins it to get the advertiser id and name --

--- This table may be used for reporting if we need to show ad geo and ranks of competitive geos ---
-- Make sure that when selecting geo you are aware there will be doubles
-- (i.e. some geos are tied to more than one advertiser id and thus, will repeat --

--this is outdated now and replaced by mei_competitive_set--


begin;
delete from &{pipeline_schema}.mei_ad_competitive_geo_set;

INSERT INTO   &{pipeline_schema}.mei_ad_competitive_geo_set (
  	ADVERTISER_ID,
  	ADVERTISER_NAME,
  	AD_NAME_FORMATTED,
  	AD_GEO_ID,
  	AD_GEO_NAME,
  	SIMILAR_GEO_ID,
  	SIMILAR_GEO_NAME,
  	SIMILARITY,
  	RANK
  	)
SELECT ag.advertiser_id             AS ADVERTISER_ID,
       adname.advertiser_name       AS ADVERTISER_NAME,
       adname.ad_name_formatted     AS AD_NAME_FORMATTED,
       c.ad_geo                     AS AD_GEO_ID,
       c.ad_geo_name                AS AD_GEO_NAME,
       c.similar_geo                AS SIMILAR_GEO_ID,
       c.sim_geo_name               AS SIMILAR_GEO_NAME,
       c.similarity                 AS SIMILARITY,
       c.rank                       AS RANK
FROM &{pipeline_schema}.mei_competitive_geo_set c
LEFT JOIN (SELECT geo_id, dfp_advertiser_id as advertiser_id FROM display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.GEO_ID = c.ad_geo
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on adname.advertiser_id = ag.advertiser_id
;

commit;


-- temp table: user_scratch.x_arosenthal.mei_competitive_geo_set_temp
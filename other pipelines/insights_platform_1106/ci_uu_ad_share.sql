
-- this table unions the two competitive geo tables to find, on each given day how many
-- unique users visited only the ad geo id, the ad geo id and a competitive geo, or only
-- a competitive geo --
-- this table adds on the advertiser_id -- make sure that when selecting geo you are aware there will be doubles
-- (i.e. some geos are tied to more than one advertiser id and thus, will repeat --

-- this table should be run 30 days behind the current date (as the source tables run
-- on a 30d lag

begin;
delete from &{pipeline_schema}.ci_uu_ad_share
where ds between &{start_dt_minus_30} and &{end_dt_minus_30};

INSERT INTO   &{pipeline_schema}.ci_uu_ad_share (
  	ds,
  	geo_id,
  	geo_name,
  	view_type,
  	uniques,
  	advertiser_id,
  	advertiser_name,
  	ad_name_formatted
  	)
SELECT u.ds                     AS DS,
       u.geo_id                 AS GEO_ID,
       u.geo_name               AS GEO_NAME,
       u.view_type              AS VIEW_TYPE,
       u.uniques                AS UNIQUES,
       ag.advertiser_id         AS ADVERTISER_ID,
       adname.advertiser_name   AS ADVERTISER_NAME,
       adname.ad_name_formatted AS AD_NAME_FORMATTED
FROM (
            SELECT c.ds                                               as ds,
                    c.geo_id                                          as geo_id,
                    c.geo_name                                        as geo_name,
                    (case when c.competitor_geo_id is not null
                        then 'Ad Geo and Competitor(s)'
                        else 'Ad Geo Only' end)                     as view_type,
                   count(distinct c.unique_id)                        as uniques
            FROM DISPLAY_ADS.sales.geos_by_unique_to_competitor c
            JOIN  &{pipeline_schema}.mei_competitive_set s ON s.ad_geo_id = c.geo_id
            JOIN rio_sf.rust.a_unique_users auu on c.unique_id = auu.unique_id and c.ds = auu.ds and auu.ds between &{start_dt_minus_30} and &{end_dt_minus_30}
            WHERE c.ds between &{start_dt_minus_30} and &{end_dt_minus_30}
            GROUP BY c.ds,
                     c.geo_id,
                     c.geo_name,
                     view_type
            UNION ALL
            SELECT cg.ds_visit_competitor                              as ds,
                    cg.geo_id                                          as geo_id,
                    cg.geo_name                                        as geo_name,
                    'Competitor(s) Only'                            as view_type,
                   count(distinct cg.unique_id)                        as uniques
            FROM DISPLAY_ADS.sales.competitor_geos_by_unique cg
            JOIN rio_sf.rust.a_unique_users auu on cg.unique_id = auu.unique_id and cg.ds_visit_competitor = auu.ds and auu.ds between &{start_dt_minus_30} and &{end_dt_minus_30}
            WHERE ds_visit_competitor between &{start_dt_minus_30} and &{end_dt_minus_30}
            GROUP BY ds_visit_competitor,
                     geo_id,
                     geo_name
            ) u
LEFT JOIN (SELECT geo_id, dfp_advertiser_id as advertiser_id from  display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.geo_id = u.geo_id
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on adname.advertiser_id = ag.advertiser_id
;

commit;

--this is the edited, new mei_competitive_set--

begin;
delete from &{pipeline_schema}.mei_competitive_set;

INSERT INTO   &{pipeline_schema}.mei_competitive_set
SELECT cs.advertiser_id                                                     as advertiser_id,
       an.advertiser_name                                                   as advertiser_name,
       an.ad_name_formatted                                                 as ad_name_formatted,
       cs.ad_geo_id                                                         as ad_geo_id,
       ag.geo_name                                                          as ad_geo_name,
       cs.similar_geo_id                                                    as similar_geo_id,
       sg.geo_name                                                          as similar_geo_name,
       sgs.region1_name                                                     as sim_geo_state,
       cs.similarity                                                        as similarity,
       rank() over (partition by cs.ad_geo_id order by cs.similarity desc)  as rank
FROM display_ads.sales.mei_all_competitve_geos cs
LEFT JOIN display_ads.sales.dfp_ad_name an on an.advertiser_id = cs.advertiser_id
LEFT JOIN tripdna.revops.dna_geo_hierarchy ag on ag.geo_id = cs.ad_geo_id
LEFT JOIN tripdna.revops.dna_geo_hierarchy sg on sg.geo_id = cs.similar_geo_id
LEFT JOIN (select * from tripdna.revops.dna_geo_hierarchy
                where country_id = '191'
                and (GEO_PLACETYPE = 'City'
                or (GEO_PLACETYPE = 'Region' and geo_original_placetypeid <> '10003'))) sgs on sgs.geo_id = cs.similar_geo_id
;

commit;
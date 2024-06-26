--- this joins the mei_all_competitive_geo set from the
--- machine learning output and outputs the top 10 competitive
--- geos per each ad geo.

--- This table will not be used for reporting because it does  not associate to advertiser ids, but is used for the
-- competitive geo tables (DISPLAY_ADS.sales.geos_by_unique_to_competitor and DISPLAY_ADS.sales.competitor_geos_by_unique)
-- in the displayads_competitive pipeline ---

-- THIS CAN BE RUN ONCE FOR THE YEAR AND THEN BE COMMENTED OUT OF THE PIPELINE UNTIL THE ML LIST IS UPDATED --

--this is outdated now and replaced by mei_competitive_set--

begin;
delete from &{pipeline_schema}.mei_competitive_geo_set;

INSERT INTO   &{pipeline_schema}.mei_competitive_geo_set
SELECT  a.ad_geo,
        a.ad_geo_name,
        a.similar_geo,
        a.sim_geo_name,
        a.sim_geo_state,
        a.similarity,
        a.rank
FROM (
    SELECT cs.ad_geo                                                    AS ad_geo,
           adg.geo_name                                                 AS ad_geo_name,
           cs.similar_geo                                               AS similar_geo,
           sg.geo_name                                                  AS sim_geo_name,
           sgs.region1_name                                             as sim_geo_state,
           cs.similarity                                                AS similarity,
           rank() over (partition by ad_geo order by similarity desc)   AS rank
    FROM DISPLAY_ADS.sales.mei_all_competitve_geos cs
    LEFT JOIN tripdna.revops.dna_geo_hierarchy adg on adg.geo_id = cs.ad_geo
    LEFT JOIN tripdna.revops.dna_geo_hierarchy sg on sg.geo_id = cs.similar_geo
    LEFT JOIN (select * from tripdna.revops.dna_geo_hierarchy
                where country_id = '191'
                and (GEO_PLACETYPE = 'City'
                or (GEO_PLACETYPE = 'Region' and geo_original_placetypeid <> '10003'))) sgs on sgs.geo_id = cs.similar_geo
    ORDER BY ad_geo asc, similarity desc) a
WHERE rank <= 10
ORDER BY ad_geo asc, similarity desc
;

commit;


-- temp table: user_scratch.x_arosenthal.mei_competitive_geo_set_temp
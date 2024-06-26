--- for each ad geo and competitive geo in the ML output (mei_competitive_geo_set)
--- this table returns all the location ids corresponding to each geo
--- This table will not be used for reporting ---

-- WANT THIS TABLE TO REPLACE ITSELF EVERY DAY WITH THE NEWEST DS --

begin;
delete from &{snow_database}.&{pipeline_schema}.ci_comp_bubble_score;

INSERT INTO   &{pipeline_schema}.ci_comp_bubble_score
select bsa.ds                                                   as ds,
       cs.ad_geo_id                                             as ad_geo,
       cs.ad_geo_name                                           as ad_geo_name,
       ag.advertiser_id                                         as advertiser_id,
       adname.advertiser_name                                   as advertiser_name,
       adname.ad_name_formatted                                 as ad_name_formatted,
       adcat.advertiser_labels                                  as advertiser_category,
       bsa.placetype                                            as ad_geo_placetype,
       bsa.num_locations                                        as ad_geo_num_loc,
       bsa.w_avg_score                                          as ad_geo_bubble_score,
       bsa.num_reviews                                          as ad_geo_num_reviews,
       cs.rank                                                  as comp_rank,
       cs.similar_geo_id                                        as similar_geo,
       cs.similar_geo_name                                      as sim_geo_name,
       cs.sim_geo_state                                         as sim_geo_state,
       bsc.placetype                                            as sim_geo_placetype,
       bsc.num_locations                                        as sim_geo_num_loc,
       bsc.w_avg_score                                          as sim_geo_bubble_score,
       bsc.num_reviews                                          as sim_geo_num_reviews
from &{pipeline_schema}.mei_competitive_set cs
left join &{pipeline_schema}.ci_bubble_score bsa on bsa.geo_id = cs.ad_geo_id
left join &{pipeline_schema}.ci_bubble_score bsc on bsc.geo_id = cs.similar_geo_id and bsa.placetype = bsc.placetype
left join (select geo_id, dfp_advertiser_id as advertiser_id from  display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.GEO_ID = cs.ad_geo_id
left join RIO_SF.DISPLAY_SALES.F_DFP_ADVERTISERS adcat on adcat.id = ag.advertiser_id
left join &{pipeline_schema}.dfp_ad_name adname on adname.advertiser_id = ag.advertiser_id
;

commit;


-- current test table: user_scratch.x_arosenthal.ci_comp_bubble_score_temp
-- USER_SCRATCH.x_arosenthal.ci_comp_bs_daily_test
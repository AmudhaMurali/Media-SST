--- for each ad geo and competitive geo in the ML output (mei_competitive_geo_set)
--- this table returns all the location ids corresponding to each geo
--- This table will not be used for reporting ---

-- WANT THIS TABLE TO REPLACE ITSELF EVERY DAY WITH THE NEWEST DS --

begin;
delete from &{pipeline_schema}.ci_bubble_score;

INSERT INTO   &{pipeline_schema}.ci_bubble_score (
  	DS,
  	GEO_ID,
  	GEO_NAME,
  	PLACETYPE,
  	NUM_LOCATIONS,
  	W_AVG_SCORE,
  	NUM_REVIEWS
  	)
WITH max_ds as (select max(ds) ds from rio_sf.hotels_sst.t_bubble_score_daily)
SELECT bsd.ds                                                                   AS DS,
       g.geo_id                                                                 AS GEO_ID,
       gn.geo_name                                                              AS GEO_NAME,
       CASE WHEN lt.place_type = 'Accomodation' THEN 'Accomodation'
            WHEN lt.place_type = 'Eatery' THEN 'Eatery'
            WHEN lt.place_type IN ('Attraction', 'Activity') THEN 'Attraction'
            ELSE 'Other'
            END                                                                 AS PLACETYPE,
       count(LOCATIONID)                                                        AS NUM_LOCATIONS,
       round((sum(bsd.score*bsd.responses)/sum(bsd.RESPONSES)),0)               AS W_AVG_SCORE,
       sum(bsd.RESPONSES)                                                       AS NUM_REVIEWS
FROM rio_sf.hotels_sst.t_bubble_score_daily bsd
    JOIN max_ds on max_ds.ds = bsd.ds
    LEFT JOIN tripdna.uni.location_tree lt on lt.location_id = bsd.LOCATIONID
    JOIN &{pipeline_schema}.geo_to_location g on g.location_id = bsd.LOCATIONID
    LEFT JOIN tripdna.revops.dna_geo_hierarchy gn on gn.geo_id = g.geo_id
GROUP BY bsd.ds,
         g.geo_id,
         gn.geo_name,
         PLACETYPE
;

commit;


-- current test table: user_scratch.x_arosenthal.ci_bubble_score_temp
--  USER_SCRATCH.x_arosenthal.ci_bs_daily_test
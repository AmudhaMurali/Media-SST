--- for each ad geo, this table aggregates the number of accommodation bookings
--- in the geo for the past 90 days, along with the weighted average star rating
--- for the bookings, the median star rating, and the mode star rating. Keep in
--- mind that some geos are associated to more than one advertiser and some
--- advertiser's have more than one geo

-- WANT THIS TABLE TO REPLACE ITSELF EVERY DAY WITH THE PAST 90D OF DATA --

begin;
delete from &{pipeline_schema}.ad_hotel_star_ratings_90d;

INSERT INTO  &{pipeline_schema}.ad_hotel_star_ratings_90d
SELECT ag.advertiser_id                                             as advertiser_id,
       adname.advertiser_name                                       as advertiser_name,
       adname.ad_name_formatted                                     as ad_name_formatted,
       g.geo_id                                                     as geo_id,
       gname.geo_name                                               as geo_name,
       sum(hb.action_count)                                         as acc_bookings,
       (sum(hb.action_count*loc.star_rating)/sum(hb.action_count))  as w_avg_star_rating,
       median(loc.star_rating)                                      as median_star_rating,
       mode(loc.star_rating)                                        as mode_star_rating
FROM DISPLAY_ADS.sales.user_location_hotel_clicks_bookings hb
JOIN (select distinct location_id, geo_id from &{pipeline_schema}.geo_to_location) g on g.location_id = hb.location_id
LEFT JOIN (select distinct geo_id, geo_name from tripdna.revops.dna_geo_hierarchy) gname on g.geo_id = gname.geo_id
JOIN (select lt.location_id,
               lt.property_name,
               lt.star_rating,
               lt.place_type,
               lt.hotel_type,
               lt.hotel_category
        FROM tripdna.uni.location_tree lt
        WHERE lower(lt.place_type) = 'accomodation'
        AND lt.star_rating is not null) loc on loc.location_id = g.location_id
JOIN (SELECT DISTINCT geo_id, dfp_advertiser_id as advertiser_id FROM display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.GEO_ID = g.geo_id
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on adname.advertiser_id = ag.ADVERTISER_ID
WHERE hb.ds between '&{start_dt_m90}' and '&{start_dt}'
AND lower(hb.action_type) = 'booking'
GROUP BY ag.ADVERTISER_ID, g.geo_id, gname.geo_name, adname.advertiser_name, adname.ad_name_formatted
;

commit;
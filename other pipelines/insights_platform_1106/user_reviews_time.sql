------------------------------------------------------------------
-- tracks reviews in advertiser geo over time (and breaks out by reviewer market and location reviewed market)
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.user_reviews_time
where ds = '&{start_dt_m1}';

insert into &{pipeline_schema}.user_reviews_time
SELECT to_date(ur.TSPUBLISHED)                                                                  as ds,
        ag.advertiser_id                                                                        as advertiser_id,
        adname.advertiser_name                                                                  as advertiser_name,
        adname.ad_name_formatted                                                                as ad_name_formatted,
       (case when ur.LOCATIONTYPE = '10022' then 'Restaurant'
           when ur.LOCATIONTYPE = '10023' then 'Accommodation'
           when ur.LOCATIONTYPE in ('10021','10043') then 'Attraction' -- Attraction, Tour
           else 'Other' end)                                                                    as poi_type, --  National Park, City, Car Rental
       ur.LOCATIONID                                                                            as location_id,
       g.geo_id                                                                                 as loc_geo_id,
       lt.country_id                                                                            as loc_country_id,
       lt.country_primaryname                                                                   as loc_country_name,
       mem.ipgeo                                                                                as user_geo_id,
       ltg.country_id                                                                           as user_country_id,
       ltg.country_primaryname                                                                  as user_country_name,
       (case when lt.country_id = ltg.country_id then 'Domestic' else 'Foreign' end)            as tvlr_type,
       ur.RATING                                                                                as user_rating,
       ur.lang                                                                                  as lang
FROM rio_sf.public.t_userreview ur
JOIN rio_sf.public.t_member mem on mem.memberid = ur.MEMBERID
JOIN &{pipeline_schema}.geo_to_location g on g.location_id = ur.LOCATIONID
JOIN tripdna.uni.location_tree lt on lt.location_id = ur.LOCATIONID -- the review location
JOIN tripdna.uni.location_tree ltg on ltg.location_id = mem.ipgeo -- the reviewER's location
JOIN (SELECT DISTINCT geo_id, dfp_advertiser_id as advertiser_id FROM display_ads.sales.gdoc_advertiser_geo_mapping) ag on ag.geo_id = g.geo_id
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on adname.advertiser_id = ag.ADVERTISER_ID
where to_date(ur.TSPUBLISHED)  = '&{start_dt_m1}'
and ur.STATUS = '4' -- published
and ur.PAGETYPE = '1'-- user review
;

commit;

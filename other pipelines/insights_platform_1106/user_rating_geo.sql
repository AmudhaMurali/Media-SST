------------------------------------------------------------------
-- tracks reviews in geo over time (and breaks out by reviewer market and location reviewed market)
------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.user_ratings_geo
where ds between '&{start_dt_m1}' and '&{end_dt}';

insert into &{pipeline_schema}.user_ratings_geo
select to_date(ur.tspublished)                                                           as ds,
           g.geo_id                                                                      as geo_id,      -- of location
           g.geo_name                                                                    as geo_name,
           lt.country_id                                                                 as loc_country_id,
           lt.country_name                                                               as loc_country_name,
           (case when ur.locationtype = '10022' then 'Restaurant'
                when ur.locationtype = '10023' then 'Accommodation'
                when ur.locationtype in ('10021', '10043') then 'Attraction' else 'Other' end)   as poi_type,
           ltg.country_id                                                                as user_country_id,
           ltg.country_name                                                              as user_country_name,
           ctr.region                                                                    as user_market,
           (case when lt.country_id = ltg.country_id then 'Domestic' else 'Foreign' end) as tvlr_type,
           round(avg(case when ur.rating = '0' then null else ur.rating end),2)                      as traveler_rating,
           (case when ur.pagetype in ('1', '4') then 'User Review'
                when ur.pagetype = '2' then 'Owner Response'
                when ur.pagetype = '3' then 'Photo Submission'
                when ur.pagetype = '5' then 'VR Traveler'
                when ur.pagetype = '6' then 'Automated Review' else 'Other' end)         as review_type,
            (case when ur.segments = '1' and ur.pagetype = '1' then 'business'
                when ur.segments = '2' and ur.pagetype = '1' then 'couples'
                when ur.segments = '4' and ur.pagetype = '1' then 'family'
                when ur.segments = '8' and ur.pagetype = '1' then 'friend'
                when ur.segments = '16' and ur.pagetype = '1' then 'solo' else null end) as segment_type,
           count(ur.id)                                                                  as num_reviews
    from rio_sf.public.t_userreview ur
             join rio_sf.public.t_member mem on mem.memberid = ur.memberid
             JOIN &{pipeline_schema}.geo_to_location g on g.location_id = ur.locationid
             JOIN &{pipeline_schema}.blt_geo_list bltg on bltg.geo_id = g.geo_id
             --JOIN tripdna.uni.location_tree lt on lt.location_id = ur.locationid -- the review location
             --JOIN tripdna.uni.location_tree ltg on ltg.location_id = mem.ipgeo -- the reviewER's location
             left join (select * from rio_sf.hotels_sst.a_geo_details_daily where ds = (select max(ds) from rio_sf.hotels_sst.a_geo_details_daily)) lt on g.geo_id = lt.geo_id -- review location
             left join (select * from rio_sf.hotels_sst.a_geo_details_daily where ds = (select max(ds) from rio_sf.hotels_sst.a_geo_details_daily)) ltg on mem.ipgeo = ltg.geo_id -- reviewer's location
             LEFT JOIN rio_sf.anm.country_to_region ctr on ltg.country_name = ctr.country
    where ur.STATUS = '4' -- published
    and to_date(ur.tspublished) between '&{start_dt_m1}' and '&{end_dt}'
    group by to_date(ur.tspublished), g.geo_id, g.geo_name, lt.country_id, lt.country_name, poi_type,
          ltg.country_id, ltg.country_name, ctr.region, tvlr_type, review_type, segment_type
;

commit;

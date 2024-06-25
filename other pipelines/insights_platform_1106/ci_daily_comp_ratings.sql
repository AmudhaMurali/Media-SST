-- Every day takes the ratings and avg bubble score for the ad geo and the average across the entire competitive set

begin;
delete from &{pipeline_schema}.ci_daily_comp_ratings
where ds = '&{start_dt_m1}';

INSERT INTO &{pipeline_schema}.ci_daily_comp_ratings
with geo_ratings as (
    select to_date(ur.tspublished)                                                                          as ds,
           g.geo_id                                                                                         as geo_id,
           gn.geo_name                                                                                      as geo_name,
           (case when ur.locationtype = '10022' then 'Restaurant'
            when ur.locationtype = '10023' then 'Accommodation'
            when ur.locationtype in ('10021', '10043') then 'Attraction' else 'Other' end)                  as poi_type,
           (case when lt.country_primaryname = ltg.country_primaryname then 'Domestic'
            when lt.country_primaryname is null then null when ltg.country_primaryname is null then null
            else 'Foreign' end)                                                                             as tvlr_type,
           avg(case when ur.rating = '0' then null else ur.rating end)                                      as avg_traveler_rating,
           count(ur.id)                                                                                     as num_reviews
    from rio_sf.public.t_userreview ur
    join rio_sf.public.t_member mem on mem.memberid = ur.memberid
    join &{pipeline_schema}.geo_to_location g on g.location_id = ur.locationid
    join tripdna.revops.dna_geo_hierarchy gn on g.geo_id = gn.geo_id
    join tripdna.uni.location_tree lt on lt.location_id = ur.locationid -- the review location
    join tripdna.uni.location_tree ltg on ltg.location_id = mem.ipgeo -- the reviewER's location
    where ur.STATUS = '4' -- published
    and ur.pagetype in ('1', '4') -- user review
    and to_date(ur.tspublished) = '&{start_dt_m1}'
    group by to_date(ur.tspublished), g.geo_id, gn.geo_name, (case when ur.locationtype = '10022' then 'Restaurant'
            when ur.locationtype = '10023' then 'Accommodation' when ur.locationtype in ('10021', '10043') then
            'Attraction' else 'Other' end), (case when lt.country_primaryname = ltg.country_primaryname then 'Domestic'
           when lt.country_primaryname is null then null  when ltg.country_primaryname is null then null else 'Foreign' end))
SELECT ra.ds                                        as ds,
           s.advertiser_id                          as advertiser_id,
           s.advertiser_name                        as advertiser_name,
           s.ad_name_formatted                      as ad_name_formatted,
           ra.geo_id                                as ad_geo_id,
           ra.geo_name                              as ad_geo_name,
           ra.poi_type                              as poi_type,
           ra.tvlr_type                             as tvlr_type,
           round(median(ra.avg_traveler_rating),2)  as ag_traveler_rating,
           cast(median(ra.num_reviews) as int)      as ag_num_reviews,
           round(avg(rc.avg_traveler_rating),2)     as cs_avg_traveler_rating,
           cast(avg(rc.num_reviews) as int)         as cs_avg_num_reviews
FROM &{pipeline_schema}.mei_competitive_set s
JOIN geo_ratings ra on s.ad_geo_id = ra.geo_id
FULL OUTER JOIN geo_ratings rc on s.similar_geo_id = rc.geo_id and rc.ds = ra.ds
                                                    and rc.poi_type = ra.poi_type
                                                    and rc.tvlr_type = ra.tvlr_type
WHERE ra.ds = '&{start_dt_m1}'
group by ra.ds, s.advertiser_id, s.advertiser_name, s.ad_name_formatted, ra.geo_id, ra.geo_name,
         ra.poi_type, ra.tvlr_type
;

commit;
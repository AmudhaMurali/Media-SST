-- This table takes each geo in the "geo_to_location" file (aka Baseline Trends geos) and records the most viewed cities for each geo/source market combo
-- by unique id rank for the past 90 days


begin;
delete from &{pipeline_schema}.blt_top_cities_geo
;

INSERT INTO &{pipeline_schema}.blt_top_cities_geo
SELECT * FROM (
        SELECT bltg.geo_id                                                                                                              as geo_id,
               bltg.geo_name                                                                                                            as geo_name,
               bltg.user_country_name                                                                                                   as user_country_name,
               ctr.region                                                                                                               as user_market,
               (case when bltg.location_id = bltg.loc_city_id then bltg.loc_region_id else bltg.loc_city_id end)                        as top_city_id,
               (case when bltg.property_name = bltg.loc_city_name then bltg.loc_region_name else bltg.loc_city_name end)                as top_city_name,
               sum(bltg.uniques)                                                                                                        as uniques,
               sum(bltg.pvs)                                                                                                            as pvs,
               sum(bltg.bookings)                                                                                                       as bookings,
               sum(bltg.clicks)                                                                                                         as clicks,
               rank() over(partition by bltg.geo_id, bltg.geo_name, bltg.user_country_name, ctr.region order by sum(bltg.uniques) desc) as city_rank
        FROM &{pipeline_schema}.blt_top_locations_geo bltg
            JOIN &{pipeline_schema}.blt_geo_list g on bltg.geo_id = g.geo_id
            JOIN rio_sf.anm.country_to_region ctr on ctr.country = bltg.user_country_name
        WHERE bltg.ds BETWEEN '&{start_dt_m90}' and '&{start_dt}'
        GROUP BY bltg.geo_id, bltg.geo_name, bltg.user_country_name, ctr.region, top_city_id, top_city_name)
WHERE city_rank BETWEEN '1' and '50'
    ;

commit;
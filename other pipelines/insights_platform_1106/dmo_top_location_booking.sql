
begin;

delete from &{pipeline_schema}.dmo_top_location_booking
where ds between '&{start_dt}' and '&{end_dt}';


INSERT INTO &{pipeline_schema}.dmo_top_location_booking

with total as (SELECT date_trunc('MONTH',bltg.ds)                                                                                              as month,
               (case when bltg.user_country_name  in ('United States', 'Canada', 'United Kingdom', 'Ireland', 'France', 'Germany',
                                                'Spain', 'Italy', 'Australia',' New Zealand', 'Japan', 'Singapore')
                                                then bltg.user_country_name else 'others' end)                                          as user_country_name_agg,
               ctr.region                                                                                                               as user_market,
               sum(bltg.uniques)                                                                                                        as uniques,
               sum(bltg.pvs)                                                                                                            as pvs,
               sum(bltg.bookings)                                                                                                       as bookings,
               sum(bltg.clicks)                                                                                                         as clicks
        FROM DISPLAY_ADS.sales.blt_top_locations_geo bltg
            --JOIN  DISPLAY_ADS.sales.blt_geo_list g on bltg.geo_id = g.geo_id
            JOIN rio_sf.anm.country_to_region ctr on ctr.country = bltg.user_country_name
        WHERE month BETWEEN '&{start_dt}' and '&{end_dt}'  -- manually changed to pull 2020-2022
        GROUP BY 1,2,3

    )

select a.month as ds, a.user_country_name_agg, a.user_market, a.top_city_id, a.top_city_name, a.uniques, a.pvs, a.bookings, a.clicks,a.city_rank_booking, total.bookings as total_bookings from
(SELECT * FROM (
        SELECT date_trunc('MONTH',bltg.ds)                                                                                              as month,
               (case when bltg.user_country_name  in ('United States', 'Canada', 'United Kingdom', 'Ireland', 'France', 'Germany',
                                                'Spain', 'Italy', 'Australia',' New Zealand', 'Japan', 'Singapore')
                                                then bltg.user_country_name else 'others' end)                                          as user_country_name_agg,
               ctr.region                                                                                                               as user_market,
               (case when bltg.location_id = bltg.loc_city_id then bltg.loc_region_id else bltg.loc_city_id end)                        as top_city_id,
               (case when bltg.property_name = bltg.loc_city_name then bltg.loc_region_name else bltg.loc_city_name end)                as top_city_name,
               sum(bltg.uniques)                                                                                                        as uniques,
               sum(bltg.pvs)                                                                                                            as pvs,
               sum(bltg.bookings)                                                                                                       as bookings,
               sum(bltg.clicks)                                                                                                         as clicks,
               rank() over(partition by month, user_country_name_agg, ctr.region order by sum(bltg.bookings) desc) as city_rank_booking
        FROM DISPLAY_ADS.sales.blt_top_locations_geo bltg
            --JOIN  DISPLAY_ADS.sales.blt_geo_list g on bltg.geo_id = g.geo_id
            JOIN rio_sf.anm.country_to_region ctr on ctr.country = bltg.user_country_name
        WHERE month  BETWEEN '&{start_dt}' and '&{end_dt}'  -- manually changed to pull 2020-2022
        GROUP BY 1,2,3,4,5)
WHERE city_rank_booking BETWEEN '1' and '50') a
left join total on a.month = total.month and a.user_country_name_agg = total.user_country_name_agg and a.user_market = total.user_market

;

commit;
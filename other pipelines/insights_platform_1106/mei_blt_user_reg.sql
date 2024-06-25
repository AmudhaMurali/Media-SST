-----------------------------------------------------------------------------
-- uses baseline_trends base to aggregate data for the top 10 user regions
-----------------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.baseline_trends_uregion
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.baseline_trends_uregion
select ds                                                                   as ds,
       t.os_group                                                           as os_group,
       t.geo_id                                                             as geo_id,
       t.geo_name                                                           as geo_name,
       t.user_country_id                                                    as user_country_id,
       t.user_country                                                       as user_country,
       ctr.region                                                           as user_market,
       case when r.user_region is null then null else t.user_region_id end  as u_region_id,
       r.user_region                                                        as user_region,
       r.user_reg_per_country_rank                                          as user_reg_per_country_rank,
       t.place_type_grouping                                                as place_type_grouping,
       sum(t.uniques_count)                                                 as uniques,
       sum(t.pvs)                                                           as pvs,
       sum(t.clicks)                                                        as clicks,
       sum(t.bookings)                                                      as bookings
from &{pipeline_schema}.baseline_trends_base t
join &{pipeline_schema}.blt_geo_list g on t.geo_id = g.geo_id
left join &{pipeline_schema}.blt_top_ureg r on r.geo_id = t.geo_id
                                            and r.user_country = t.user_country
                                            and r.user_region = t.user_region
left join rio_sf.anm.country_to_region ctr on t.user_country = ctr.country
where ds between '&{start_dt}' and '&{end_dt}'
group by ds, t.os_group, t.geo_id, t.geo_name, user_country_id, t.user_country, ctr.region, u_region_id,
         r.user_region, r.user_reg_per_country_rank, t.place_type_grouping
order by geo_name, user_country, user_reg_per_country_rank
;

commit;

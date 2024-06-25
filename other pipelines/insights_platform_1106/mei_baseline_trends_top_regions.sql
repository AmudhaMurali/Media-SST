------------------------------------------------------------------------
-- uses baseline_trends base to aggregate data for the top 10 user regions
------------------------------------------------------------------------

-- need to change this to not pull from baseline trends base

begin;

delete from &{pipeline_schema}.blt_top_ureg;

insert into &{pipeline_schema}.blt_top_ureg
with top_user_countries as (
        select * from (
            select bltb.geo_id,
                   bltb.geo_name,
                   bltb.user_country,
                   sum(bltb.uniques_count) as uniques,
                   rank() over(partition by bltb.geo_id order by sum(bltb.uniques_count) desc) as rank
            from &{pipeline_schema}.baseline_trends_base bltb -- changed to new base table
            join &{pipeline_schema}.blt_geo_list g on g.geo_id = bltb.geo_id
            where bltb.ds between '2019-01-01' and '2019-12-31'
            and bltb.user_country is not null
            group by bltb.geo_id, bltb.geo_name, bltb.user_country)
        where rank between '1' and '5')
select a.geo_id             as geo_id,
       a.geo_name           as geo_name,
       uc.rank              as user_country_rank,
       a.user_country       as user_country,
       a.rank               as user_reg_per_country_rank,
       a.user_region        as user_region,
       a.uniques            as uniques,
       a.geo_uniques        as geo_uniques,
       a.country_uniques    as country_uniques,
       a.region_uniques     as region_uniques
from (
        select btb.geo_id,
               btb.geo_name,
               btb.user_country,
               btb.user_region,
               sum(btb.uniques_count) as uniques,
               rank() over(partition by btb.geo_id, btb.user_country order by sum(btb.uniques_count) desc) as rank,
               sum(uniques) over(partition by btb.geo_id) as geo_uniques,
               sum(uniques) over(partition by btb.geo_id, btb.user_country) as country_uniques,
               sum(uniques) over(partition by btb.geo_id, btb.user_country, btb.user_region) as region_uniques
        from &{pipeline_schema}.baseline_trends_base btb -- changed to new base table
        join &{pipeline_schema}.blt_geo_list g on g.geo_id = btb.geo_id
        where btb.ds between '2019-01-01' and '2019-12-31'
        and btb.user_country is not null
        and btb.user_region is not null
        group by btb.geo_id, btb.geo_name, btb.user_country, btb.user_region) a
join top_user_countries uc on uc.geo_id = a.geo_id and uc.user_country = a.user_country
where a.rank between '1' and '10'
order by a.geo_name, uc.rank, a.rank;

;

commit;

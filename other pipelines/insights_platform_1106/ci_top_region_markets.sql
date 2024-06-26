-- Top 50 region source market traffic for each ad geo and across its competitive set in the past 90 days


begin;
delete from &{pipeline_schema}.ci_top_region_markets;

INSERT INTO &{pipeline_schema}.ci_top_region_markets
with top_fifty_ad_region_markets as (
    select * from (
            select geo_id,
            	   geo_name,
            	   user_region,
            	   user_country,
            	   sum(uniques) as uniques,
            	   rank() over(partition by geo_id order by sum(uniques) desc) as rank
            from rio_sf.cx_analytics.ugeo_traffic_bookings_daily
            where user_region is not null
            and ds between '&{start_dt_m90}' and '&{start_dt}'
            group by  geo_id, geo_name, user_region, user_country)
        where rank between '1' and '50'),
top_fifty_comp_region_markets as (
    select * from (
            select s.advertiser_id,
                   s.advertiser_name,
                   s.ad_name_formatted,
                   s.ad_geo_id,
                   s.ad_geo_name,
                   user_region,
                   user_country,
                   sum(uniques) as uniques,
                   rank() over(partition by s.ad_geo_id order by sum(uniques) desc) as rank
            FROM &{pipeline_schema}.mei_competitive_set s
            join rio_sf.cx_analytics.ugeo_traffic_bookings_daily ug on s.similar_geo_id = ug.geo_id
            where ds between '&{start_dt_m90}' and '&{start_dt}'
            and ug.user_region is not null
            group by s.advertiser_id, s.advertiser_name, s.ad_name_formatted, s.ad_geo_id, s.ad_geo_name, user_region, user_country)
        where rank between '1' and '50')
select cm.advertiser_id,
       cm.advertiser_name,
       cm.ad_name_formatted,
       cm.ad_geo_id,
       am.geo_id,
       am.geo_name,
       am.rank as ad_rank,
       am.user_region as ad_user_region,
       am.user_country as ad_country_of_reg,
       am.uniques as ad_uniques,
       cm.rank as comp_rank,
       cm.user_region as comp_user_region,
       cm.user_country as comp_country_of_reg,
       cm.uniques as comp_uniques
from top_fifty_ad_region_markets am
join top_fifty_comp_region_markets cm on cm.ad_geo_id = am.geo_id and cm.ad_geo_name = am.geo_name and am.rank = cm.rank
;

commit;
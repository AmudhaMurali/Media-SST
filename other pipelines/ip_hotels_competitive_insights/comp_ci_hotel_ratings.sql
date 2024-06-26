begin;

delete from &{pipeline_schema}.comp_ci_hotel_ratings
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.comp_ci_hotel_ratings

with comp_match as (
        select distinct brand_code,
                        brand,
                        parent_brand,
                        comp_brand_code,
                        property_country_name as comp_for_country
        from &{pipeline_schema}.ip_hotel_comp_set)

select cb.ds,
       'Competitive Brand' as brand_type,
       cm.brand,
       cm.brand_code,
       cm.parent_brand,
       cm.comp_for_country,
       cb.star_rating,
       round(avg(cb.management_responses),0) as management_responses,
       round(avg(cb.review_count),0) as review_count,
       round(avg(cb.published_photo_count),0) as published_photo_count,
       round(avg(cb.published_video_count),0) as published_video_count,
       round(avg(cb.avg_bubble_score),1) as avg_bubble_score,
       round(avg(cb.rate_cleanliness),1) as rate_cleanliness,
       round(avg(cb.rate_location),1) as rate_location,
       round(avg(cb.rate_room),1) as rate_room,
       round(avg(cb.rate_service),1) as rate_service,
       round(avg(cb.rate_sleep),1) as rate_sleep,
       round(avg(cb.rate_value),1) as rate_value,
       round(avg(cb.num_property_ratings),1) as num_property_ratings,
       round(avg(cb.management_responses_pp),1) as management_responses_pp,
       round(avg(cb.review_count_pp),1) as review_count_pp,
       round(avg(cb.published_photo_count_pp),1) as published_photo_count_pp,
       round(avg(cb.published_video_count_pp),1) as published_video_count_pp
from &{pipeline_schema}.ci_hotel_ratings cb
join comp_match  cm on cb.brand_code = cm.comp_brand_code
where cb.ds between '&{start_dt}' and '&{end_dt}'
and cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Competitive Brand', cm.brand, cm.brand_code, cm.parent_brand, cm.comp_for_country, cb.star_rating

union all

select cb.ds,
       'Primary Brand' as brand_type,
       cmp.brand,
       cmp.brand_code,
       cmp.parent_brand,
       cmp.comp_for_country,
       cb.star_rating,
       round(avg(cb.management_responses),0) as management_responses,
       round(avg(cb.review_count),0) as review_count,
       round(avg(cb.published_photo_count),0) as published_photo_count,
       round(avg(cb.published_video_count),0) as published_video_count,
       round(avg(cb.avg_bubble_score),1) as avg_bubble_score,
       round(avg(cb.rate_cleanliness),1) as rate_cleanliness,
       round(avg(cb.rate_location),1) as rate_location,
       round(avg(cb.rate_room),1) as rate_room,
       round(avg(cb.rate_service),1) as rate_service,
       round(avg(cb.rate_sleep),1) as rate_sleep,
       round(avg(cb.rate_value),1) as rate_value,
       round(avg(cb.num_property_ratings),1) as num_property_ratings,
       round(avg(cb.management_responses_pp),1) as management_responses_pp,
       round(avg(cb.review_count_pp),1) as review_count_pp,
       round(avg(cb.published_photo_count_pp),1) as published_photo_count_pp,
       round(avg(cb.published_video_count_pp),1) as published_video_count_pp

from &{pipeline_schema}.ci_hotel_ratings cb
join comp_match  cmp on cb.brand_code = cmp.brand_code
where cb.ds between '&{start_dt}' and '&{end_dt}'
and cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Primary Brand', cmp.brand, cmp.brand_code, cmp.parent_brand, cmp.comp_for_country, cb.star_rating

;

commit;

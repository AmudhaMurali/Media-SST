begin;

delete from &{pipeline_schema}.ci_hotel_ratings
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.ci_hotel_ratings

--with max_ds as (select max(ds) as ds from rio_sf.hotels_sst.a_hotel_details_daily)
select ahdd.ds as ds,
       hex_encode(lower(ahdd.brand_name)) as brand_code,
       ahdd.brand_name,
       ahdd.parent_brand_name,
       --ahdd.sales_region,
       aldd.country_id as property_country_id,
       aldd.country as property_country_name,
       round((ahdd.star_rating/2),1) as star_rating,
       sum(aldd.management_responses) as management_responses,
       sum(aldd.review_count) as review_count,
       sum(aldd.published_photo_count) as published_photo_count,
       sum(aldd.published_video_count) as published_video_count,
       round(avg(aldd.bubble_score/10),1) as avg_bubble_score,
       round(avg(cast(aldd.location_subratings:RATE_CLEANLINESS as int)/10),1) as rate_cleanliness,
       round(avg(cast(aldd.location_subratings:RATE_LOCATION as int)/10),1) as rate_location,
       round(avg(cast(aldd.location_subratings:RATE_ROOM as int)/10),1) as rate_room,
       round(avg(cast(aldd.location_subratings:RATE_SERVICE as int)/10),1) as rate_service,
       round(avg(cast(aldd.location_subratings:RATE_SLEEP as int)/10),1) as rate_sleep,
       round(avg(cast(aldd.location_subratings:RATE_VALUE as int)/10),1) as rate_value,
       count(distinct aldd.location_id)  as num_property_ratings,
       sum(aldd.management_responses)/count(distinct aldd.location_id) as management_responses_pp,
       sum(aldd.review_count)/count(distinct aldd.location_id) as review_count_pp,
       sum(aldd.published_photo_count)/count(distinct aldd.location_id) as published_photo_count_pp,
       sum(aldd.published_video_count)/count(distinct aldd.location_id) as published_video_count_pp
from rio_sf.hotels_sst.a_hotel_details_daily ahdd
--join max_ds on max_ds.ds = ahdd.ds
join rio_sf.hotels_sst.a_location_details_daily aldd on aldd.location_id = ahdd.location_id
                                                     and aldd.ds = ahdd.ds
                                                     --and aldd.ds = max_ds.ds
                                                     and aldd.active = 1
                                                     and aldd.permanently_closed = 0
                                                     and ahdd.brand_name is not null
where ahdd.ds between '&{start_dt}' and '&{end_dt}'
group by ahdd.ds, hex_encode(lower(ahdd.brand_name)), ahdd.brand_name, ahdd.parent_brand_name, aldd.country_id,
         aldd.country, round((ahdd.star_rating/2),1), ahdd.sales_region

union all

select ahdd.ds as ds,
       hex_encode(lower(ahdd.brand_name)) as brand_code,
       ahdd.brand_name,
       ahdd.parent_brand_name,
       --null as sales_region,
       '1' as property_country_id,
       'Overall Brand' as property_country_name,
       round((ahdd.star_rating/2),1) as star_rating,
       sum(aldd.management_responses) as management_responses,
       sum(aldd.review_count) as review_count,
       sum(aldd.published_photo_count) as published_photo_count,
       sum(aldd.published_video_count) as published_video_count,
       round(avg(aldd.bubble_score/10),1) as avg_bubble_score,
       round(avg(cast(aldd.location_subratings:RATE_CLEANLINESS as int)/10),1) as rate_cleanliness,
       round(avg(cast(aldd.location_subratings:RATE_LOCATION as int)/10),1) as rate_location,
       round(avg(cast(aldd.location_subratings:RATE_ROOM as int)/10),1) as rate_room,
       round(avg(cast(aldd.location_subratings:RATE_SERVICE as int)/10),1) as rate_service,
       round(avg(cast(aldd.location_subratings:RATE_SLEEP as int)/10),1) as rate_sleep,
       round(avg(cast(aldd.location_subratings:RATE_VALUE as int)/10),1) as rate_value,
       count(distinct aldd.location_id)  as num_property_ratings,
       sum(aldd.management_responses)/count(distinct aldd.location_id) as management_responses_pp,
       sum(aldd.review_count)/count(distinct aldd.location_id) as review_count_pp,
       sum(aldd.published_photo_count)/count(distinct aldd.location_id) as published_photo_count_pp,
       sum(aldd.published_video_count)/count(distinct aldd.location_id) as published_video_count_pp
from rio_sf.hotels_sst.a_hotel_details_daily ahdd
--join max_ds on max_ds.ds = ahdd.ds
join rio_sf.hotels_sst.a_location_details_daily aldd on aldd.location_id = ahdd.location_id
                                                     and aldd.ds = ahdd.ds
                                                     --and aldd.ds = max_ds.ds
                                                     and aldd.active = 1
                                                     and aldd.permanently_closed = 0
                                                     and ahdd.brand_name is not null
where ahdd.ds between '&{start_dt}' and '&{end_dt}'
group by ahdd.ds, hex_encode(lower(ahdd.brand_name)), ahdd.brand_name, ahdd.parent_brand_name,
          '1', 'Overall Brand', round((ahdd.star_rating/2),1)

;

commit;
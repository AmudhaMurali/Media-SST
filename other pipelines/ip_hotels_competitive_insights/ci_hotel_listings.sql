begin;

delete from &{pipeline_schema}.ci_hotel_listings
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.ci_hotel_listings

--with max_ds as (select max(ds) as ds from rio_sf.hotels_sst.a_hotel_details_daily)
select ahdd.ds as ds,
       hex_encode(lower(ahdd.brand_name)) as brand_code,
       ahdd.brand_name,
       ahdd.parent_brand_name,
       --ahdd.sales_region,
       aldd.country_id as property_country_id,
       aldd.country as property_country_name,
       round((ahdd.star_rating/2),1) as star_rating,
       count(distinct ahdd.location_id) as num_properties
from rio_sf.hotels_sst.a_hotel_details_daily ahdd
--join max_ds on max_ds.ds = ahdd.ds
join rio_sf.hotels_sst.a_location_details_daily aldd on aldd.location_id = ahdd.location_id
                                                     and aldd.ds = ahdd.ds
                                                     --and aldd.ds = max_ds.ds
                                                     and ahdd.brand_name is not null
where ahdd.ds between '&{start_dt}' and '&{end_dt}'
group by ahdd.ds, hex_encode(lower(ahdd.brand_name)), ahdd.brand_name, ahdd.parent_brand_name,
          aldd.country_id, aldd.country, round((ahdd.star_rating/2),1)

union all

select ahdd.ds as ds,
       hex_encode(lower(ahdd.brand_name)) as brand_code,
       ahdd.brand_name,
       ahdd.parent_brand_name,
       --'Overall Brand'    as sales_region,
       1 as property_country_id,
       'Overall Brand'     as property_country_name,
       round((ahdd.star_rating/2),1) as star_rating,
       count(distinct ahdd.location_id) as num_properties
from rio_sf.hotels_sst.a_hotel_details_daily ahdd
--join max_ds on max_ds.ds = ahdd.ds
join rio_sf.hotels_sst.a_location_details_daily aldd on aldd.location_id = ahdd.location_id
                                                     and aldd.ds = ahdd.ds
                                                     --and aldd.ds = max_ds.ds
                                                     and ahdd.brand_name is not null
where ahdd.ds between '&{start_dt}' and '&{end_dt}'
group by ahdd.ds, hex_encode(lower(ahdd.brand_name)), ahdd.brand_name, ahdd.parent_brand_name, round((ahdd.star_rating/2),1)

;

commit;
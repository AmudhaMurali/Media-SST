begin;

delete from &{pipeline_schema}.ci_hotel_market;

insert into &{pipeline_schema}.ci_hotel_market

--with max_ds as (select max(ds) as ds from rio_sf.hotels_sst.a_hotel_details_daily)
select
    '&{start_dt}' as ds,
    code       as brand_code,
    brand_name              ,
    parent_brand_name       ,
    hotel_country_id       as property_country_id,
    hotel_country_name     as property_country_name,
    --sales_region            ,
    os_group                ,
    user_country_id         ,
    user_country_name       ,
    user_region_id          ,
    user_region_name        ,
    sum(uniques)  as uniques
from DISPLAY_ADS.INSIGHTS_PLATFORM_HOTELS.tc_all_location_traffic
where ds between '&{start_dt_m90}' and '&{start_dt}'
group by 1,2,3,4,5,6,7,8,9,10,11

union all

select
    '&{start_dt}' as ds,
    code       as brand_code,
    brand_name              ,
    parent_brand_name       ,
    1       as property_country_id,
    'Overall Brand'     as property_country_name,
    --'Overall Brand'     as sales_region            ,
    os_group                ,
    user_country_id         ,
    user_country_name       ,
    user_region_id          ,
    user_region_name        ,
    sum(uniques)  as uniques
from DISPLAY_ADS.INSIGHTS_PLATFORM_HOTELS.tc_all_location_traffic
where ds between '&{start_dt_m90}' and '&{start_dt}'
group by 1,2,3,4,5,6,7,8,9,10,11

;

commit;

begin;
delete from &{pipeline_schema_sf}.hotel_advertiser_location_mapping;

insert into &{pipeline_schema_sf}.hotel_advertiser_location_mapping


select distinct cast (dfp_advertiser_id as int) as dfp_advertiser_id ,
                advertiser_name,
                ad_name_formatted,
                location_id,
                brand_name,
                parent_brand_name,
                sales_region_requested as sales_region,
                HOTEL_COUNTRY_NAME,
                HOTEL_STATE_NAME
                from (
select distinct hamt.dfp_advertiser_id,
                dan.advertiser_name,
                dan.ad_name_formatted,
                hamt.hotel_brand_code,
                ihb.location_id,
                ihb.property_name,
                ihb.brand_name,
                ihb.parent_brand_name,
                ihb.sales_region,
                ihb.COUNTRY_NAME AS HOTEL_COUNTRY_NAME,
                ihb.STATE_NAME AS HOTEL_STATE_NAME,
                hamt.sales_region as sales_region_requested,
                to_boolean(case when hamt.sales_region = ihb.sales_region then 1
                           when hamt.sales_region = 'Global' then 1
                           else 0 end) as include_location
from display_ads.sales.gdoc_hotel_advertiser_mapping hamt
join display_ads.insights_platform_hotels.ip_hotel_brands ihb on hamt.hotel_brand_code = ihb.code
left join display_ads.sales.dfp_ad_name dan on hamt.dfp_advertiser_id = dan.advertiser_id)
where include_location = 1

;

commit;


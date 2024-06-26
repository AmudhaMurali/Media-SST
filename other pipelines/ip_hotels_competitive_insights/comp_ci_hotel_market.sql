begin;

delete from &{pipeline_schema}.comp_ci_hotel_market;

insert into &{pipeline_schema}.comp_ci_hotel_market

with comp_match as (
        select distinct brand_code,
                        brand,
                        parent_brand,
                        comp_brand_code,
                        property_country_name as comp_for_country
        from &{pipeline_schema}.ip_hotel_comp_set)

select
       cb.ds,
       'Competitive Brand' as brand_type,
       cm.brand,
       cm.brand_code,
       cm.parent_brand,
       cm.comp_for_country,
       cb.os_group                ,
       cb.user_country_id         ,
       cb.user_country_name       ,
       cb.user_region_id          ,
       cb.user_region_name        ,
       round(avg(cb.uniques),1)  as uniques

from &{pipeline_schema}.ci_hotel_market cb
join comp_match  cm on cb.brand_code = cm.comp_brand_code
where  cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Competitive Brand', cm.brand, cm.brand_code, cm.parent_brand, cm.comp_for_country,
         cb.os_group, cb.user_country_id, cb.user_country_name, cb.user_region_id, cb.user_region_name

union all

select cb.ds,
       'Primary Brand' as brand_type,
       cmp.brand,
       cmp.brand_code,
       cmp.parent_brand,
       cmp.comp_for_country,
       cb.os_group                ,
       cb.user_country_id         ,
       cb.user_country_name       ,
       cb.user_region_id          ,
       cb.user_region_name        ,
       round(avg(cb.uniques),1)  as uniques

from &{pipeline_schema}.ci_hotel_market cb
join comp_match  cmp on cb.brand_code = cmp.brand_code
where cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Primary Brand', cmp.brand, cmp.brand_code, cmp.parent_brand, cmp.comp_for_country,
         cb.os_group, cb.user_country_id, cb.user_country_name, cb.user_region_id, cb.user_region_name
;

commit;
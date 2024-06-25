begin;

delete from &{pipeline_schema}.comp_ci_hotel_metrics
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.comp_ci_hotel_metrics

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
       --cb.user_country_name,
       round(avg(cb.uniques),1)        as uniques,
       round(avg(cb.pageviews),1)      as pageviews,
       round(avg(cb.clicks)   ,1)      as clicks,
       round(avg(cb.est_bookings) ,1)  as est_bookings,
       round(avg(cb.bookings),1)       as bookings,
       round(avg(cb.avg_nights_per_booking),1) as avg_nights_per_booking,
       round(avg(cb.avg_nightly_spend),2) as avg_nightly_spend,
       round(avg(cb.avg_num_guests),1) as avg_num_guests,
       round(avg(cb.avg_num_rooms),1) as avg_num_rooms,
       round(avg(cb.avg_days_out),1) as avg_days_out,
       round(avg(cb.avg_properties_viewed_pp),2) as avg_properties_viewed_pp,
       round(avg(cb.num_property_metrics),2)  as num_property_metrics,
       round(avg(cb.uniques_pp),2)  as uniques_pp,
       round(avg(cb.pageviews_pp),2)  as pageviews_pp,
       round(avg(cb.clicks_pp),2)  as clicks_pp,
       round(avg(cb.est_bookings_pp),2)  as est_bookings_pp,
       round(avg(cb.bookings_pp),2)  as bookings_pp

from &{pipeline_schema}.ci_hotel_metrics cb
join comp_match  cm on cb.brand_code = cm.comp_brand_code
where cb.ds between '&{start_dt}' and '&{end_dt}'
and cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Competitive Brand', cm.brand, cm.brand_code, cm.parent_brand, cm.comp_for_country

union all

select cb.ds,
       'Primary Brand' as brand_type,
       cmp.brand,
       cmp.brand_code,
       cmp.parent_brand,
       cmp.comp_for_country,
       --cb.user_country_name,
       round(avg(cb.uniques),1)        as uniques,
       round(avg(cb.pageviews),1)      as pageviews,
       round(avg(cb.clicks)   ,1)      as clicks,
       round(avg(cb.est_bookings) ,1)  as est_bookings,
       round(avg(cb.bookings),1)       as bookings,
       round(avg(cb.avg_nights_per_booking),1) as avg_nights_per_booking,
       round(avg(cb.avg_nightly_spend),2) as avg_nightly_spend,
       round(avg(cb.avg_num_guests),1) as avg_num_guests,
       round(avg(cb.avg_num_rooms),1) as avg_num_rooms,
       round(avg(cb.avg_days_out),1) as avg_days_out,
       round(avg(cb.avg_properties_viewed_pp),2) as avg_properties_viewed_pp,
       round(avg(cb.num_property_metrics),2)  as num_property_metrics,
       round(avg(cb.uniques_pp),2)  as uniques_pp,
       round(avg(cb.pageviews_pp),2)  as pageviews_pp,
       round(avg(cb.clicks_pp),2)  as clicks_pp,
       round(avg(cb.est_bookings_pp),2)  as est_bookings_pp,
       round(avg(cb.bookings_pp),2)  as bookings_pp

from &{pipeline_schema}.ci_hotel_metrics cb
join comp_match  cmp on cb.brand_code = cmp.brand_code
where cb.ds between '&{start_dt}' and '&{end_dt}'
and cb.property_country_name = 'Overall Brand'
group by cb.ds, 'Primary Brand', cmp.brand, cmp.brand_code, cmp.parent_brand, cmp.comp_for_country

;

commit;
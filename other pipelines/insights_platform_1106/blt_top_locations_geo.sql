-- This table takes each geo in the "geo_to_location" file (aka Baseline Trends geos) and records the page views, bookings, and clicks
-- for each location wihtin the geo (heirarchy) so that a user can calculate the top viewed locations and top viewed cities/regions
-- within each of the geos


begin;
delete from &{pipeline_schema}.blt_top_locations_geo
where ds = '&{start_dt}';

INSERT INTO &{pipeline_schema}.blt_top_locations_geo
SELECT ulid.ds                                                                                      as ds,
       (case when uu.os_type in ('android_browser','iphone_browser','other_phone') then 'Mobile Web'
            when uu.os_type in ('android_native_app','ipad_native_app','iphone_native_app') then 'Native App'
            when uu.os_type in ('android_tablet_browser','ipad_browser','other_tablet') then 'Tablet Web'
            when uu.os_type in ('linux','osx','other', 'windows') then 'Desktop' else 'Other' end)  as os_group,
       uu.user_location_id                                                                          as user_location_id,
       uu.commerce_country_id                                                                       as user_country_id,
       uu.user_country_name                                                                         as user_country_name,
       ulid.location_id                                                                             as location_id,
       lt.property_name                                                                             as property_name,
       lt.city_id                                                                                   as loc_city_id,
       lt.city_primaryname                                                                          as loc_city_name,
       lt.region_id                                                                                 as loc_region_id,
       lt.region_primaryname                                                                        as loc_region_name,
       g.geo_id                                                                                     as geo_id,  -- of the looked at/booked location
       g.geo_name                                                                                   as geo_name,
       ulid.simple_placetype_name                                                                   as place_type,
       count(distinct ulid.unique_id)                                                               as uniques,
       sum(case when ulid.action_type='pageviews' then ulid.action_count else 0 end)                as pvs,
       sum(case when ulid.action_type='booking' then ulid.action_count else 0 end)                  as bookings,
       sum(case when ulid.action_type='click' then ulid.action_count else 0 end)                    as clicks
FROM DISPLAY_ADS.sales.vw_user_location_interactions_daily ulid
JOIN (select unique_id,
        max(os_type) as os_type,
        max(user_ip_location_id) as user_location_id,
        max(c.country_id) as commerce_country_id,
        max(user_ip_country_name) as user_country_name
          from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS u
              left join rio_sf.public.country_id_to_country c on c.COUNTRY=u.USER_IP_COUNTRY_NAME
          where ds between '&{start_dt_m30}' and '&{start_dt}'
          group by 1
        ) uu on uu.unique_id = ulid.unique_id
JOIN &{pipeline_schema}.geo_to_location g on g.location_id = ulid.location_id -- geo id and geo_name
--LEFT JOIN tripdna.revops.dna_geo_hierarchy gn on gn.geo_id = g.geo_id -- geo name
JOIN tripdna.uni.location_tree lt on ulid.location_id = lt.location_id -- location detail
WHERE ulid.ds = '&{start_dt}'
AND ulid.simple_placetype_name <> 'Other'
GROUP BY ulid.ds, ulid.location_id, lt.property_name,lt.city_id, lt.city_primaryname,
         lt.region_id, lt.region_primaryname, g.geo_id, g.geo_name, ulid.simple_placetype_name,
         os_group, uu.user_location_id, uu.commerce_country_id, uu.user_country_name
;

commit;
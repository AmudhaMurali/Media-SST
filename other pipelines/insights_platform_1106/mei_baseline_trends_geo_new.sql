------------------------------------------------------------------------
-- uses location interactions as base table and counts the interations
-- (i.e. pageviews, meta bookings, clicks, instant bookings) within the
-- selected geos. Joins to a_unique_users to get user info (like user
-- location)
------------------------------------------------------------------------

begin;

delete from &{pipeline_schema}.baseline_trends_base
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.baseline_trends_base
select a.ds                                                                                        as ds,
       uu.OS_TYPE                                                                                  as os_type,
       (case when uu.os_type in ('android_browser','iphone_browser','other_phone') then 'Mobile Web'
            when uu.os_type in ('android_native_app','ipad_native_app','iphone_native_app') then 'Native App'
            when uu.os_type in ('android_tablet_browser','ipad_browser','other_tablet') then 'Tablet Web'
            when uu.os_type in ('linux','osx','other', 'windows') then 'Desktop' else 'Other' end) as os_group,
       geo2.HIERARCHICAL_LOCATION_ID                                                               as geo_id,
       geo2.HIERARCHICAL_LOCATION_NAME                                                             as geo_name,
       geo2.HIERARCHICAL_PLACETYPE_NAME                                                            as geo_depth,
       a.SIMPLE_PLACETYPE_NAME                                                                     as place_type_grouping,
       ugeo2.continent_name                                                                        as user_continent,
       ugeo2.country_id                                                                            as user_country_id,
       (case when ugeo2.country_name is null then uu.user_country_name else ugeo2.country_name end) as user_country,
       ugeo2.region1_id                                                                            as user_region_id,
       ugeo2.region1_name                                                                          as user_region,
       ugeo2.geo_id                                                                                as user_city_id,
       ugeo2.geo_name                                                                              as user_city,
       sum(CASE WHEN a.action_type = 'pageviews' THEN a.action_count ELSE 0 END)                   as PVS,
       sum(CASE WHEN a.action_type = 'click' THEN a.action_count ELSE 0 END)                       as clicks,       -- click on meta link
       sum(CASE WHEN a.action_type = 'booking' THEN a.action_count ELSE 0 END)                     as bookings,
       count(distinct a.UNIQUE_ID)                                                                 as uniques_count,
       concat(geo2.HIERARCHICAL_PLACETYPE_NAME,'_',geo2.HIERARCHICAL_LOCATION_NAME)                as DMO_TYPE
FROM display_ads.sales.vw_user_location_interactions_daily a
JOIN (select unique_id, max(os_type) as os_type, max(user_ip_location_id) as user_location_id, max(user_ip_country_name) as user_country_name
          from USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS
          where ds between '&{start_dt_m30}' and '&{end_dt}'
          group by 1
        ) uu on uu.unique_id = a.unique_id
left join tripdna.revops.dna_geo_hierarchy ugeo2 on uu.USER_LOCATION_ID = ugeo2.geo_id -- Simon's Hierarchy (User Geo)
LEFT JOIN display_ads.sales.location_hierarchy geo2 on a.LOCATION_ID = geo2.LOCATION_ID -- Jeff's Hierarchy (Loc Geo)
--JOIN &{pipeline_schema}.blt_geo_list g on g.geo_id = geo2.HIERARCHICAL_LOCATION_ID
where a.ds between '&{start_dt}' and '&{end_dt}'
group by a.ds, uu.OS_TYPE, os_group, geo2.HIERARCHICAL_LOCATION_ID, geo2.HIERARCHICAL_LOCATION_NAME,
         geo2.HIERARCHICAL_PLACETYPE_NAME, a.SIMPLE_PLACETYPE_NAME, ugeo2.continent_name, ugeo2.country_id,
         user_country, ugeo2.region1_id, ugeo2.region1_name, ugeo2.geo_id, ugeo2.geo_name, DMO_TYPE
         ;

commit;

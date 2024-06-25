
begin;
delete from &{pipeline_schema}.blt_ip_pageviews
where ds between '&{start_dt}' and '&{end_dt}';

INSERT INTO  &{pipeline_schema}.blt_ip_pageviews
WITH geo_to_location_pt as (
            SELECT DISTINCT lh.location_id  as location_id,
            lh.location_name                as location_name,
            lh.location_placetype_name      as location_placetype_name,
            lh.hierarchical_location_id     as geo_id,
            lh.hierarchical_location_name   as geo_name,
            lh.hierarchical_placetype_name  as geo_placetype_name
            FROM display_ads.sales.location_hierarchy lh
            WHERE hierarchical_location_id <> 1
            AND hierarchical_depth <> 0)
SELECT pv.ds                                                    as ds,
       g.geo_id                                                 as geo_id,
       g.geo_name                                               as geo_name,
       split_part(split_part(navigationstring, '|', 3),':',3)   as in_country,
       split_part(split_part(navigationstring, '|', 4),':',3)   as in_reg,
       g.geo_placetype_name                                     as geo_placetype_name,
       agdd.country_id                                          as  user_country_id,
       agdd.country_name                                        as  user_country_name,
       count(distinct pv.unique_id)                             as uniques,
       sum(pv.action_count)                                     as page_views
FROM display_ads.sales.user_location_pageviews pv
JOIN USER_TRACKING.PUBLIC.BLESSED_UNIQUE_USERS uu on pv.unique_id = uu.unique_id
                                   and pv.ds = uu.ds
                                   and uu.ds between '&{start_dt}' and '&{end_dt}'
left join rio_sf.public.country_id_to_country c on c.COUNTRY=uu.USER_IP_COUNTRY_NAME
LEFT JOIN (select distinct ds, country_id, country_name from rio_sf.hotels_sst.a_geo_details_daily)  agdd
    on c.country_id = agdd.country_id and agdd.ds = (select max(ds) as ds from rio_sf.hotels_sst.a_geo_details_daily)
LEFT JOIN geo_to_location_pt g on pv.location_id = g.location_id
LEFT JOIN rio_sf.public.t_location l on g.geo_id = l.id
WHERE pv.ds between '&{start_dt}' and '&{end_dt}'
GROUP BY pv.ds, g.geo_id, g.geo_name, g.geo_placetype_name, agdd.country_id, agdd.country_name,
         split_part(split_part(navigationstring, '|', 3),':',3),
         split_part(split_part(navigationstring, '|', 4),':',3)

;

commit;
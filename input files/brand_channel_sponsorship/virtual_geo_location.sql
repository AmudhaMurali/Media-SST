INSERT OVERWRITE into &{pipeline_schema_sf}.virtual_geo_location

SELECT '&{start_dt}'            as last_updated,
       v.regionid               as virtual_geo_id,
       l1.primaryname           as virtual_geo_name,
       l1.placetypeid           as virtual_geo_placetypeid,
       pt1.name                 as virtual_geo_placetype_name,
       v.childid                as child_id, -- will be different from location_id if child is a geo
       l2.primaryname           as child_id_name,
       l2.placetypeid           as child_id_placetypeid,
       pt2.name                 as child_id_placetypename,
       lp.locationid            as location_id,
       l3.primaryname           as location_id_name,
       l3.placetypeid           as location_id_placetypeid,
       pt3.name                 as location_id_placetypename
FROM rio_sf.hotels_demand.t_virtual_region v
LEFT JOIN rio_sf.public.t_locationpaths lp on v.childid = lp.parentid
JOIN rio_sf.public.t_location l1 on v.regionid = l1.id
JOIN rio_sf.public.t_location l2 on v.childid = l2.id
JOIN rio_sf.public.t_location l3 on lp.locationid = l3.id
LEFT JOIN rio_sf.public.t_location_closing_info c on lp.locationid = c.locationid
LEFT JOIN rio_sf.public.t_placetype pt1 on l1.placetypeid = pt1.id
LEFT JOIN rio_sf.public.t_placetype pt2 on l2.placetypeid = pt2.id
LEFT JOIN rio_sf.public.t_placetype pt3 on l3.placetypeid = pt3.id
WHERE l2.status = 4 -- published on the live site
AND c.id is null -- not closed listing

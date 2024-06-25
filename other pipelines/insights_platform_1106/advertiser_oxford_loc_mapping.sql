--- maps each advertiser geo id to the parent country geo id (to determine whether or not it will be used for economic impact ---


-- WANT THIS TABLE TO REPLACE ITSELF EVERY DAY WITH THE NEWEST DS --

begin;
delete from &{pipeline_schema}.advertiser_oxford_loc_mapping;

INSERT INTO   &{pipeline_schema}.advertiser_oxford_loc_mapping (
  	 ox_loc_id   	    ,
     ox_name            ,
     advertiser_id      ,
     advertiser_name
  	)
select  distinct first_value(ox.id) over (partition by location_id order by hierarchical_depth asc)             as ox_loc_id,
                 first_value(ox.primaryname) over (partition by location_id order by hierarchical_depth asc)    as ox_name,
                 m.dfp_advertiser_id                                                                            as advertiser_id,
                 m.dmo_name                                                                                     as advertiser_name
from display_ads.sales.gdoc_advertiser_geo_mapping m
left join rio_sf.display_sales.location_hierarchy h on m.geo_id = h.location_id
join &{pipeline_schema}.oxford_economics_country_mapping ox on h.hierarchical_location_id = ox.id
;

commit;

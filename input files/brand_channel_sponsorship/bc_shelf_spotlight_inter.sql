------------------------------------------------------------------------------------
-- Impressions for the Discovery Shelf and Destination Spotlight portion of DS
-- Does not have geo_id field so includes all geos, not only sponsored ones
-- Copied from the interactions portion of cx_analytics.mixer_shelf_analyzer table
------------------------------------------------------------------------------------

begin;

delete from &{pipeline_schema_sf}.bc_shelf_spotlight_inter
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_shelf_spotlight_inter

select
    ui.ds,
    ui.unique_id,
    trim(ui.ui_element_keys:feedSectionId,'"') as feed_section_id,
    trim(ui.ui_element_keys:puid,'"') as puid,
    trim(ui.ui_element_keys:context,'"') as context,
    trim(ui.ui_element_keys:element,'"') as element,
    ui.ui_element_type,
    ui.ui_element_source
from USER_TRACKING.PUBLIC.F_USER_INTERACTION_DEPRECATED ui
where ui.ds between '&{start_dt}' and '&{end_dt}'
and ui.ui_element_source = 'Mixer' and ui.ui_element_type not in ('feedScroll');

commit;


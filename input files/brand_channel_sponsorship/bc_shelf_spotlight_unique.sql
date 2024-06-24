begin;

delete from &{pipeline_schema_sf}.bc_shelf_spotlight_unique
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_shelf_spotlight_unique

select imp.ds,
       imp.unique_id,
       imp.marketing_campaign_id,
       imp.user_country_id,
       imp.user_country_name,
       imp.bc_geo_id,
       imp.bc_geo_name,
       imp.os_type,
       imp.locale,
       imp.bc_shelf,
       count(distinct imp.puid) as impressions,
       count(distinct case when (int.ui_element_source = 'Mixer'
                                 and int.ui_element_type in ('feedClick','seeAll','forumsCtaClick','shelfScroll','batchGalleryScroll',
                                                             'linkedPoiScroll','linkedPoiClick','batchGalleryClick'))
                                then int.feed_section_id end) as interactions
                           -- any interaction with the shelf - horizontal scroll, shelf scroll, see all click, click on the shelf, etc.
from &{pipeline_schema_sf}.bc_shelf_spotlight_imps imp
left join &{pipeline_schema_sf}.bc_shelf_spotlight_inter int on imp.unique_id = int.unique_id
                                                          and imp.ds = int.ds
                                                          and (imp.feed_section_id = int.feed_section_id)
where imp.ds between '&{start_dt}' and '&{end_dt}'
group by imp.ds,
         imp.unique_id,
         imp.marketing_campaign_id,
         imp.user_country_id,
         imp.user_country_name,
         imp.bc_geo_id,
         imp.bc_geo_name,
         imp.os_type,
         imp.locale,
         imp.bc_shelf

union all

select ds,
       unique_id,
       marketing_campaign_id,
       user_country_id,
       user_country_name,
       bc_geo_id,
       bc_geo_name,
       os_type,
       locale,
       bc_shelf,
       impressions,
       interactions
from &{pipeline_schema_sf}.bc_shelf_spotlight_update
where ds between '&{start_dt}' and '&{end_dt}'


;

commit;

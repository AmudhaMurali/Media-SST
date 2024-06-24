begin;

delete from &{pipeline_schema_sf}.bc_shelf_spotlight_update
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema_sf}.bc_shelf_spotlight_update

with impressions as (
    select imp.ds,
           imp.UNIQUE_ID,
           au.os_type,
           imp.locale,
           au.marketing_campaign_id,
           au.commerce_country_id as user_country_id,
           au.user_country_name,
           imp.geo_id as bc_geo_id,
           bc.sponsor_name as bc_geo_name,
           imp.PAGE_UID as puid,
           (case when imp.custom_data_json:placementTemplate in ('INSET_IMAGE_FEATURE_CARD', 'FULL_IMAGE_FEATURE_CARD') and (imp.custom_data_json:titleKey like 'brand_channel_hero_card_' or imp.custom_data_json:titleKey = 'brand_channel_hero_card') then 'brand_channel_hero_card1'
            when imp.custom_data_json:placementTemplate in ('INSET_IMAGE_FEATURE_CARD', 'FULL_IMAGE_FEATURE_CARD') and imp.custom_data_json:titleKey like 'brand_channel_hero_card2_' then 'brand_channel_hero_card2'
            when imp.custom_data_json:placementTemplate in ('INSET_IMAGE_FEATURE_CARD', 'FULL_IMAGE_FEATURE_CARD') and imp.custom_data_json:titleKey like 'brand_channel_hero_card3_' then 'brand_channel_hero_card3'
            when imp.custom_data_json:placementTemplate = 'INTERACTIVE_MAP' and (imp.custom_data_json:titleKey like 'brand_channel_map_%' or imp.custom_data_json:titleKey = 'brand_channel_map') then 'brand_channel_map1'
            when imp.custom_data_json:placementTemplate = 'INTERACTIVE_MAP' and imp.custom_data_json:titleKey like 'brand_channel_map2_%' then 'brand_channel_map2'
            when imp.custom_data_json:placementTemplate = 'INTERACTIVE_MAP' and imp.custom_data_json:titleKey like 'brand_channel_map3_%' then 'brand_channel_map3'
            when imp.custom_data_json:placementTemplate = 'UGC_CARD_SHELF' and (imp.custom_data_json:titleKey like 'brand_channel_discovery_carousel_%' or imp.custom_data_json:titleKey = 'brand_channel_discovery_carousel') then 'brand_channel_discovery_carousel1'
            when imp.custom_data_json:placementTemplate = 'UGC_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_discovery_carousel2_%' then 'brand_channel_discovery_carousel2'
            when imp.custom_data_json:placementTemplate = 'UGC_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_discovery_carousel3_%' then 'brand_channel_discovery_carousel3'
            when imp.custom_data_json:placementTemplate = 'EDITORIAL_FEATURE' and (imp.custom_data_json:titleKey like 'brand_channel_content_spotlight_%' or imp.custom_data_json:titleKey = 'brand_channel_content_spotlight')  then 'brand_channel_content_spotlight'
            when imp.custom_data_json:placementTemplate = 'EDITORIAL_FEATURE' and imp.custom_data_json:titleKey like 'brand_channel_content_spotlight2_%' then 'brand_channel_content_spotlight2'
            when imp.custom_data_json:placementTemplate = 'EDITORIAL_FEATURE' and imp.custom_data_json:titleKey like 'brand_channel_content_spotlight3_%' then 'brand_channel_content_spotlight3'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and (imp.custom_data_json:titleKey like 'brand_channel_central_nav_%' or imp.custom_data_json:titleKey = 'brand_channel_central_nav') then 'brand_channel_central_nav1'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_central_nav2_%' then 'brand_channel_central_nav2'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_central_nav3_%' then 'brand_channel_central_nav3'
            when imp.custom_data_json:placementTemplate = 'FORUM_CARD_SHELF' and (imp.custom_data_json:titleKey like 'brand_channel_forums_shelf_%' or imp.custom_data_json:titleKey = 'brand_channel_forums_shelf') then 'brand_channel_forums_shelf'
            when imp.custom_data_json:placementTemplate = 'FORUM_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_forums_shelf2_%' then 'brand_channel_forums_shelf2'
            when imp.custom_data_json:placementTemplate = 'FORUM_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_forums_shelf3_%' then 'brand_channel_forums_shelf3'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and (imp.custom_data_json:titleKey like 'brand_channel_reviews_%' or imp.custom_data_json:titleKey = 'brand_channel_reviews') then 'brand_channel_central_reviews'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_reviews2_%' then 'brand_channel_central_reviews2'
            when imp.custom_data_json:placementTemplate = 'VERTICAL_CARD_SHELF' and imp.custom_data_json:titleKey like 'brand_channel_reviews3_%' then 'brand_channel_central_reviews3'
            end)  as bc_shelf,
            count(distinct(imp.impression_id)) as impressions
from  user_tracking.public.user_impressions imp
join rio_sf.rust.a_unique_users au on imp.unique_id = au.unique_id and imp.ds = au.ds and (au.ds between '&{start_dt}' and '&{end_dt}')
join (select distinct location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bc on bc.location_id = imp.geo_id
where imp.team = 'TAPS'
and imp.item_type = 'SponsoredTourismImpression'
and imp.page = 'Tourism'
and imp.ds between '&{start_dt}' and '&{end_dt}'
and imp.custom_data_json:operativeOneOrderIdSponsoredTourism is null
group by 1,2,3,4,5,6,7,8,9,10,11),

interactions as (
  select int.ds,
        int.UNIQUE_ID,
        au.os_type,
        int.locale,
        au.marketing_campaign_id,
        au.commerce_country_id as user_country_id,
        au.user_country_name,
        int.geo_id as bc_geo_id,
        bc.sponsor_name as bc_geo_name,
        int.PAGE_UID as puid,
       (case when (int.custom_data_json:shelfTitleKey like 'brand_channel_hero_card_' or int.custom_data_json:shelfTitleKey = 'brand_channel_hero_card') then 'brand_channel_hero_card1'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_hero_card2_' then 'brand_channel_hero_card2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_hero_card3_' then 'brand_channel_hero_card3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_map_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_map') then 'brand_channel_map1'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_map2_%' then 'brand_channel_map2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_map3_%' then 'brand_channel_map3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_discovery_carousel_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_discovery_carousel') then 'brand_channel_discovery_carousel1'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_discovery_carousel2_%' then 'brand_channel_discovery_carousel2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_discovery_carousel3_%' then 'brand_channel_discovery_carousel3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_content_spotlight_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_content_spotlight')  then 'brand_channel_content_spotlight'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_content_spotlight2_%' then 'brand_channel_content_spotlight2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_content_spotlight3_%' then 'brand_channel_content_spotlight3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_central_nav_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_central_nav') then 'brand_channel_central_nav1'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_central_nav2_%' then 'brand_channel_central_nav2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_central_nav3_%' then 'brand_channel_central_nav3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_forums_shelf_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_forums_shelf') then 'brand_channel_forums_shelf'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_forums_shelf2_%' then 'brand_channel_forums_shelf2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_forums_shelf3_%' then 'brand_channel_forums_shelf3'
        when (int.custom_data_json:shelfTitleKey like 'brand_channel_reviews_%' or int.custom_data_json:shelfTitleKey = 'brand_channel_reviews') then 'brand_channel_central_reviews'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_reviews2_%' then 'brand_channel_central_reviews2'
        when int.custom_data_json:shelfTitleKey like 'brand_channel_reviews3_%' then 'brand_channel_central_reviews3'
        end)  as bc_shelf,
        count(distinct INTERACTION_ID) as interactions
from user_tracking.public.user_interactions int
join rio_sf.rust.a_unique_users au on int.unique_id = au.unique_id and int.ds = au.ds and (au.ds between '&{start_dt}' and '&{end_dt}')
join (select distinct location_id, sponsor_name FROM DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.t_spock_brandchannels) bc on bc.location_id = int.geo_id
where int.team = 'TAPS'
and int.page = 'Tourism'
and int.ds between '&{start_dt}' and '&{end_dt}'
and int.action_type = 'click'
and int.custom_data_json:operativeOneOrderIdSponsoredTourism is null
group by 1,2,3,4,5,6,7,8,9,10,11)

  select   imp.ds,
           imp.UNIQUE_ID,
           imp.os_type,
           imp.locale,
           imp.marketing_campaign_id,
           imp.user_country_id,
           imp.user_country_name,
           imp.bc_geo_id,
           imp.bc_geo_name,
           imp.bc_shelf,
           impressions,
           interactions
  from impressions imp
  left join interactions int on imp.unique_id = int.unique_id and imp.puid=int.puid and imp.bc_shelf = int.bc_shelf


;

commit;
DELETE FROM &{pipeline_schema}.base_media_interactions
WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';


INSERT INTO &{pipeline_schema}.base_media_interactions
SELECT
    DS,
    CASE
       when (locale =  'ar_EG') then 'ar-EG-u-nu-latn'
       when (locale =  'ar') then 'ar-US-u-nu-latn'
       when (locale =  'cs') then 'cs'
       when (locale =  'da') then 'da-DK'
       when (locale =  'de_AT') then 'de-AT'
       when (locale =  'de_BE') then 'de-BE'
       when (locale =  'de_CH') then 'de-CH'
       when (locale =  'de') then 'de-DE'
       when (locale =  'el') then 'el-GR'
       when (locale =  'en_AU') then 'en-AU'
       when (locale =  'en_CA') then 'en-CA'
       when (locale =  'en_UK') then 'en-GB'
       when (locale =  'en_HK') then 'en-HK'
       when (locale =  'en_IE') then 'en-IE'
       when (locale =  'en_IN') then 'en-IN'
       when (locale =  'en_MY') then 'en-MY'
       when (locale =  'en_NZ') then 'en-NZ'
       when (locale =  'en_PH') then 'en-PH'
       when (locale =  'en_SG') then 'en-SG'
       when (locale =  'en_US') then 'en-US'
       when (locale =  'en_ZA') then 'en-ZA'
       when (locale =  'es_AR') then 'es-AR'
       when (locale =  'es_CL') then 'es-CL'
       when (locale =  'es_CO') then 'es-CO'
       when (locale =  'es') then 'es-ES'
       when (locale =  'es_MX') then 'es-MX'
       when (locale =  'es_PE') then 'es-PE'
       when (locale =  'es_VE') then 'es-VE'
       when (locale =  'fr_BE') then 'fr-BE'
       when (locale =  'fr_CA') then 'fr-CA'
       when (locale =  'fr_CH') then 'fr-CH'
       when (locale =  'fr') then 'fr-FR'
       when (locale =  'he_IL') then 'he-IL'
       when (locale =  'hu') then 'hu'
       when (locale =  'id') then 'id-ID'
       when (locale =  'it_CH') then 'it-CH'
       when (locale =  'it') then 'it-IT'
       when (locale =  'iw') then 'iw'
       when (locale =  'ja') then 'ja-JP'
       when (locale =  'ko') then 'ko-KR'
       when (locale =  'no') then 'nb-NO'
       when (locale =  'nl_BE') then 'nl-BE'
       when (locale =  'nl') then 'nl-NL'
       when (locale =  'pl') then 'pl'
       when (locale =  'pt') then 'pt-BR'
       when (locale =  'pt_PT') then 'pt-PT'
       when (locale =  'ru') then 'ru-RU'
       when (locale =  'sv') then 'sv-SE'
       when (locale =  'th') then 'th-u-ca-gregory'
       when (locale =  'tr') then 'tr-TR'
       when (locale =  'vi') then 'vi-VN'
       when (locale =  'zh_CN') then 'zh-CN'
       when (locale =  'zh') then 'zh-Hans-US'
       when (locale =  'zh_HK') then 'zh-Hant-HK'
       when (locale =  'zh_TW') then 'zh-Hant-TW'
       else locale
    END AS LOCALE,
    OS_PLATFORM,
    ITEM_ID,
    ITEM_TYPE,
    ITEM_NAME,
    ACTION_TYPE,
    ACTION_SUB_TYPE,
    CONCAT('l',ITEM_ID) AS URL_ID,
    UNIQUE_ID,
    PAGE,
    LOCATION_ID,
    USER_AGENT,
    CASE
        WHEN PAGE = 'HotelsFusion' THEN 'Hotels'
        WHEN PAGE = 'AttractionsFusion' THEN 'Attractions'
        ELSE PAGE
        END AS SERVLET,
    COALESCE(geo_id, 0) AS GEO_ID,
    MCID AS MARKETING_CAMPAIGN_ID,
    COUNT(distinct INTERACTION_ID) as TOTAL_INTERACTION,
    --sponsor map
    count_if(ITEM_NAME IN ('clickTitleLinkNonPOISponsoredCard', 'clickTitleLinkNonTripadvisorLocationSponsoredCard')) AS title_cta_click,
	count_if(ITEM_NAME IN ('clickCTALinkNonPOISponsoredCard', 'clickCTALinkNonTripadvisorLocationSponsoredCard')) AS link_cta_click,
	--article
	CASE
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')) , 'coral_content_spotlight_') then 'Content_Spotlight_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')) , 'coral_content_spotlight2_') then 'Content_Spotlight_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')) , 'coral_content_spotlight3_') then 'Content_Spotlight_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_discovery_carousel_') then 'Discovery_Carousel_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_discovery_carousel2_') then 'Discovery_Carousel_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_discovery_carousel3_') then 'Discovery_Carousel_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_destination_shelf_') then 'Destination_Shelf_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_destination_shelf2_') then 'Destination_Shelf_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_destination_shelf3_') then 'Destination_Shelf_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_poi_shelf_') then 'POI_Shelf_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_poi_shelf2_')  then 'POI_Shelf_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_poi_shelf3_')  then 'POI_Shelf_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_forum_spotlight_') then 'Forum_Spotlight_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_forum_spotlight2_') then 'Forum_Spotlight_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_forum_spotlight3_') then 'Forum_Spotlight_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_review_spotlight_') then 'Review_Spotlight_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_review_spotlight2_') then 'Review_Spotlight_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_review_spotlight3_') then 'Review_Spotlight_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_traveler_spotlight_') then 'Traveler_Spotlight_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_traveler_spotlight2_') then 'Traveler_Spotlight_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')),  'coral_traveler_spotlight3_') then 'Traveler_Spotlight_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_hero_card_') then 'Hero_Content_Card_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_hero_card2_') then 'Hero_Content_Card_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_hero_card3_') then 'Hero_Content_Card_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_map_') then 'Map_Shelf_#1'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_map2_') then 'Map_Shelf_#2'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_map3_') then 'Map_Shelf_#3'
                when  CONTAINS(lower(trim(custom_data_json:shelfTitleKey, '"')), 'coral_custom_') then 'Custom Shelf'
                else 'Others'
        end as shelf_type,

	sum(iff(item_name = 'ReefFeedScroll' , 1, 0)) shelf_imps,
    sum(iff(item_name = 'ReefFeedClick', 1, 0)) shelf_clicks


FROM USER_TRACKING.PUBLIC.USER_INTERACTIONS
WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}'
GROUP BY ALL;
-- Creating a mapping table that will update daily (so will not be distinct) with all sponsored articles and associated Operative One order ids
-- To be used for dwell time tables (because RUM doesnt have custom data tracking)

BEGIN;
DELETE FROM &{pipeline_schema_sf}.article_lookback_pageview
WHERE ds BETWEEN '&{start_dt}' and '&{end_dt}';
;

INSERT INTO &{pipeline_schema_sf}.article_lookback_pageview

with mapping_order_id_op as (
        select distinct
                cast(replace(split_part(regexp_substr(custom_data,'operativeOrderId\\D*\\d*'),'"',-1), ':', '') as int) as order_id,
                cast(split_part(regexp_substr(custom_data,'Articles-l\\w*-'),'-',2) as string) as url_id,
                ITEM_ID,
                item_name,
                item_type
        from user_tracking.public.user_impressions where ds between '&{start_dt}' and '&{end_dt}'
        and ITEM_TYPE in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink','Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
        and lower(custom_data) like '%operativeorderid%')
-- 01/22/2024 -- Added the CTEs below to capture the missing order ids
  , mapping_order_id_pio_imp_1 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_impressions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ITEM_TYPE in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink','Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
        -- The condition below is to guarantee we don't generate duplicates
        and RIGHT(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string), LEN(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string))-1) = ui.ITEM_ID
        )
  , mapping_order_id_pio_int_1 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_interactions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ITEM_TYPE in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink','Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
        -- The condition below is to guarantee we don't generate duplicates
        and RIGHT(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string), LEN(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string))-1) = ui.ITEM_ID
      )
  , mapping_order_id_pio_int_2 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_interactions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ui.team = 'TAPS' and ui.page = 'Articles'
        and ui.item_name in ('ReefFeedScroll','ReefFeedClick')
        -- The condition below is to guarantee we don't generate duplicates
        and RIGHT(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string), LEN(cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string))-1) = ui.ITEM_ID
      )
  , mapping_order_id_pio_int_3 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_interactions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ITEM_TYPE in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink','Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
      )
  , mapping_order_id_pio_int_4 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_interactions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ui.team = 'TAPS' and ui.page = 'Articles'
        and ui.item_name in ('ReefFeedScroll','ReefFeedClick')
      )
  , mapping_order_id_pio_imp_2 as (
        select distinct
                uim.order_id,
                COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) as url_id,
                ui.ITEM_ID,
                ui.item_name,
                ui.item_type
        from user_tracking.public.user_impressions  ui
        left join display_ads.sales.article_order_unique_id_mapping uim
            ON COALESCE(concat('l',ui.item_id),cast(split_part(regexp_substr(ui.custom_data,'Articles-l\\w*-'),'-',2) as string)) = uim.url_id
        where ds between '&{start_dt}' and '&{end_dt}'
        and ITEM_TYPE in ('articleSponsorInfoImpression','articleLinkClick','articleCarouselClick','articleFormSubmission','articleStickyNavigationClick','articleLink','Shelf', 'EditorialFeatureSection', 'InsetImageFeatureCardSection', 'MapSection')
        )
  , mapping_order_id as (
        select '1' as col, *
        from mapping_order_id_op

        union all

        select '2' as col, *
        from mapping_order_id_pio_imp_1

        union all

        select '3' as col, *
        from mapping_order_id_pio_int_1

        union all

        select '4' as col, *
        from mapping_order_id_pio_int_2

        union all

        select '5' as col, *
        from mapping_order_id_pio_int_3

        union all

        select '6' as col, *
        from mapping_order_id_pio_int_4

        union all

        select '7' as col, *
        from mapping_order_id_pio_imp_2
    )

select     pv.ds,
           case
           when (pv.locale =  'ar_EG') then 'ar-EG-u-nu-latn'
           when (pv.locale =  'ar') then 'ar-US-u-nu-latn'
           when (pv.locale =  'cs') then 'cs'
           when (pv.locale =  'da') then 'da-DK'
           when (pv.locale =  'de_AT') then 'de-AT'
           when (pv.locale =  'de_BE') then 'de-BE'
           when (pv.locale =  'de_CH') then 'de-CH'
           when (pv.locale =  'de') then 'de-DE'
           when (pv.locale =  'el') then 'el-GR'
           when (pv.locale =  'en_AU') then 'en-AU'
           when (pv.locale =  'en_CA') then 'en-CA'
           when (pv.locale =  'en_UK') then 'en-GB'
           when (pv.locale =  'en_HK') then 'en-HK'
           when (pv.locale =  'en_IE') then 'en-IE'
           when (pv.locale =  'en_IN') then 'en-IN'
           when (pv.locale =  'en_MY') then 'en-MY'
           when (pv.locale =  'en_NZ') then 'en-NZ'
           when (pv.locale =  'en_PH') then 'en-PH'
           when (pv.locale =  'en_SG') then 'en-SG'
           when (pv.locale =  'en_US') then 'en-US'
           when (pv.locale =  'en_ZA') then 'en-ZA'
           when (pv.locale =  'es_AR') then 'es-AR'
           when (pv.locale =  'es_CL') then 'es-CL'
           when (pv.locale =  'es_CO') then 'es-CO'
           when (pv.locale =  'es') then 'es-ES'
           when (pv.locale =  'es_MX') then 'es-MX'
           when (pv.locale =  'es_PE') then 'es-PE'
           when (pv.locale =  'es_VE') then 'es-VE'
           when (pv.locale =  'fr_BE') then 'fr-BE'
           when (pv.locale =  'fr_CA') then 'fr-CA'
           when (pv.locale =  'fr_CH') then 'fr-CH'
           when (pv.locale =  'fr') then 'fr-FR'
           when (pv.locale =  'he_IL') then 'he-IL'
           when (pv.locale =  'hu') then 'hu'
           when (pv.locale =  'id') then 'id-ID'
           when (pv.locale =  'it_CH') then 'it-CH'
           when (pv.locale =  'it') then 'it-IT'
           when (pv.locale =  'iw') then 'iw'
           when (pv.locale =  'ja') then 'ja-JP'
           when (pv.locale =  'ko') then 'ko-KR'
           when (pv.locale =  'no') then 'nb-NO'
           when (pv.locale =  'nl_BE') then 'nl-BE'
           when (pv.locale =  'nl') then 'nl-NL'
           when (pv.locale =  'pl') then 'pl'
           when (pv.locale =  'pt') then 'pt-BR'
           when (pv.locale =  'pt_PT') then 'pt-PT'
           when (pv.locale =  'ru') then 'ru-RU'
           when (pv.locale =  'sv') then 'sv-SE'
           when (pv.locale =  'th') then 'th-u-ca-gregory'
           when (pv.locale =  'tr') then 'tr-TR'
           when (pv.locale =  'vi') then 'vi-VN'
           when (pv.locale =  'zh_CN') then 'zh-CN'
           when (pv.locale =  'zh') then 'zh-Hans-US'
           when (pv.locale =  'zh_HK') then 'zh-Hant-HK'
           when (pv.locale =  'zh_TW') then 'zh-Hant-TW'
           else pv.locale
           end as locale,
               pv.USER_COUNTRY_ID,
               pv.USER_COUNTRY_NAME,
               pv.COMMERCE_COUNTRY_ID,
               pv.MARKETING_CAMPAIGN_ID,
               split_part(split_part(regexp_substr(pv.url,'source\\W+\\w+\\D\\w+'),'=',2),'&&',1) as utm_source,
               pv.OS_TYPE_NAME as OS_PLATFORM,
               cast(right(split_part(regexp_substr(pv.URL,'/Articles-l\\w*-'),'-',2), len(split_part(regexp_substr(pv.URL,'/Articles-l\\w*-'),'-',2))-1)as string) as ITEM_ID,
               map.ITEM_NAME as ITEM_NAME,
               map.ITEM_TYPE as ITEM_TYPE,
               map.order_id as order_id,
               cast(split_part(regexp_substr(pv.URL,'/Articles-l\\w*-'),'-',2) as string)  as url_id,
               pv.UNIQUE_ID
from rio_sf.anm.a_lookback_blessed_joined_only_pv pv
    left join (
               select *
               from (
                     select *,
                            ROW_NUMBER() OVER (PARTITION BY url_id ORDER BY col) as rn
                     from mapping_order_id)
               where rn = 1)  map
        on map.URL_ID = cast(split_part(regexp_substr(pv.URL,'/Articles-l\\w*-'),'-',2) as string)
where pv.URL like '%/Articles-l%' and pv.ds between '&{start_dt}' and '&{end_dt}'
;

commit;
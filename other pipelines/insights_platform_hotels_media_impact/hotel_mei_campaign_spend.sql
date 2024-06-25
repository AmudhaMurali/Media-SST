
begin;

delete from &{pipeline_schema_sf}.hotel_mei_campaign_spend;

insert into &{pipeline_schema_sf}.hotel_mei_campaign_spend

select
        sp.DS                          ,
        sp.MONTH_AND_YEAR              ,
        sp.OP_ADVERTISER_ID            ,
        sp.OP_ADVERTISER_NAME          ,
        sp.AD_SERVER                   ,
        map.advertiser_id              ,
        map.advertiser_name            ,
        map.ad_name_formatted          ,
        map.brand_name                 as ad_brand_name,
        map.parent_brand_name          as ad_parent_brand_name,
        map.sales_region               ,
        sp.CATEGORY                    ,
        sp.FORECAST_CATEGORY           ,
        sp.SALES_ORDER_ID              ,
        sp.SALES_ORDER_NAME            ,
        sp.SALES_ORDER_LINE_ITEM_ID    ,
        sp.SALES_ORDER_LINE_ITEM_NAME  ,
        sp.LINE_ITEM_START_DATE        ,
        sp.LINE_ITEM_END_DATE          ,
        sp.CURRENCY_LOCAL_REV          ,
        sp.LOCAL_REVENUE               ,
        sp.USD_REVENUE                 ,
        sp.CURRENCY_LOCAL_CONTRACT     ,
        sp.LOCAL_CONTRACTED_AMOUNT     ,
        sp.USD_CONTRACTED_AMOUNT


from display_ads.sales.mei_advertiser_spend sp
JOIN (select distinct dfp_advertiser_id as advertiser_id, advertiser_name, ad_name_formatted,brand_name, parent_brand_name,sales_region
      from &{pipeline_schema_sf}.hotel_advertiser_location_mapping) map on map.advertiser_id = sp.AD_SERVER_ADVERTISER_ID

;

commit;
--update
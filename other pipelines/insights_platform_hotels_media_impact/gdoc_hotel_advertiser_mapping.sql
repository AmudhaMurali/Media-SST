DELETE FROM ${hive_pipeline_schema}.gdoc_hotel_advertiser_mapping;

INSERT INTO ${hive_pipeline_schema}.gdoc_hotel_advertiser_mapping
SELECT  timestamp           ,
    hotel_brand_name    ,
    hotel_brand_code    ,
    dfp_advertiser_id   ,
    sales_region
FROM google_sheets.default.Hotel_Advertiser_Brand_Mapping
;
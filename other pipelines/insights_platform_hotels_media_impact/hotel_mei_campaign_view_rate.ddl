
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.hotel_mei_campaign_view_rate (
    DS                       DATE,
    IMP_TOTAL_AD_IMPRESSIONS INT,
    IMP_TOTAL_AD_CLICKS      INT,
    IMP_UNIQUES_COUNT        INT,
    NUM_SAW_CAMPAIGN         INT,
    DS_INTER                 DATE,
    DAYDIF                   INT,
    SAW_AND_LOOKED           BOOLEAN,
    NUM_SAW_AND_LOOKED       INT,
    ADVERTISER_ID            INT,
    ADVERTISER_NAME          STRING,
    AD_NAME_FORMATTED        STRING,
    AD_BRAND_NAME            STRING,
    AD_PARENT_BRAND_NAME     STRING,
    SALES_REGION             STRING,
    ADVERTISER_CATEGORY      STRING,
    USER_COUNTRY             STRING,
    USER_CONTINENT           STRING,
    USER_MARKET              STRING,
    OS_GROUP                 STRING
);


grant select on &{pipeline_schema_sf}.hotel_mei_campaign_view_rate  to public;

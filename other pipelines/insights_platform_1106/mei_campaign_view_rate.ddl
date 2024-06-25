CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_campaign_view_rate (
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
    ADVERTISER_CATEGORY      STRING,
    USER_COUNTRY             STRING,
    OS_GROUP                 STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_campaign_view_rate TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_campaign_view_rate TO PUBLIC;
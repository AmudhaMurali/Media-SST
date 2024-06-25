CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_campaign_imps_lookback (
    DS                       DATE,
    SAW_CAMPAIGN             BOOLEAN,
    IMP_TOTAL_AD_IMPRESSIONS INT,
    IMP_TOTAL_AD_CLICKS      INT,
    NUM_SAW_CAMPAIGN         INT,
    IMP_UNIQUES_COUNT        INT,
    DS_INTER                 DATE,
    DAYDIF                   INT,
    SAW_AND_LOOKED           BOOLEAN,
    NUM_SAW_AND_LOOKED       INT,
    ADVERTISER_ID            INT,
    ADVERTISER_NAME          STRING,
    AD_NAME_FORMATTED        STRING,
    ADVERTISER_CATEGORY      STRING,
    COUNTRY_NAME             STRING,
    REGION                   STRING,
    OS_TYPE                  STRING,
    OS_GROUP                 STRING,
    LOCALE                   STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_campaign_imps_lookback TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_campaign_imps_lookback TO PUBLIC;
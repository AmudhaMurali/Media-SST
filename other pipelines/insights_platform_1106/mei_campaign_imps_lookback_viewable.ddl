CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_campaign_imps_lookback_viewable (
    DS                                  DATE,
    SAW_CAMPAIGN                        BOOLEAN,
    imp_total_viewable_ad_impressions   INT,
    imp_total_viewable_ad_clicks        INT,
    NUM_SAW_CAMPAIGN                    INT,
    IMP_UNIQUES_COUNT                   INT,
    DS_INTER                            DATE,
    DAYDIF                              INT,
    SAW_AND_LOOKED                      BOOLEAN,
    NUM_SAW_AND_LOOKED                  INT,
    ADVERTISER_ID                       INT,
    ADVERTISER_NAME                     STRING,
    AD_NAME_FORMATTED                   STRING,
    ADVERTISER_CATEGORY                 STRING,
    COUNTRY_NAME                        STRING,
    REGION                              STRING,
    SUB_REGION                          STRING,
    OS_TYPE                             STRING,
    OS_GROUP                            STRING,
    LOCALE                              STRING
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_campaign_imps_lookback_viewable TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_campaign_imps_lookback_viewable TO PUBLIC;
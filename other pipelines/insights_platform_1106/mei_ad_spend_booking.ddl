CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_ad_spend_booking (
    DS                      DATE,
    BILLING_PERIOD_NAME     STRING,
    ADVERTISER_ID           INT,
    ADVERTISER_NAME         STRING,
    AD_NAME_FORMATTED       STRING,
    UNIQUES                 INT,
    ACC_BOOKINGS            INT,
    ATTR_BOOKINGS           INT,
    RECOGNIZED_REVENUE      FLOAT
);


GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_ad_spend_booking TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_ad_spend_booking TO PUBLIC;
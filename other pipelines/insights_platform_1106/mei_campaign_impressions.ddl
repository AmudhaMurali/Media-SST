CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_campaign_impressions (
        ds                              DATE,
        advertiser_id                   INT,
        advertiser_name                 STRING,
        ad_name_formatted               STRING,
        advertiser_category             STRING,
        order_id                        INT,
        native_ad_format_name           STRING,
        user_country                    STRING,
        os_group                        STRING,
        imp_uniques_count               INT,
        imp_viewable_ad_impressions     INT,
        imp_viewable_ad_clicks          INT,
        imp_total_ad_impressions        INT,
        imp_total_ad_clicks             INT

);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.mei_campaign_impressions TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.mei_campaign_impressions TO PUBLIC;
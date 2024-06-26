CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.user_hotel_advertiser_impressions_daily (
        ds                              DATE,
        advertiser_id                   INT,
        advertiser_name                 STRING,
        ad_name_formatted               STRING,
        --brand_name                      STRING,
        --parent_brand_name               STRING,
        --sales_region                    STRING,
        advertiser_category             STRING,
        order_id                        INT,
        native_ad_format_name           STRING,
        user_country                    STRING,
        user_continent                  STRING,
        user_market                     STRING,
        os_group                        STRING,
        imp_uniques_count               INT,
        imp_viewable_ad_impressions     INT,
        imp_viewable_ad_clicks          INT,
        imp_total_ad_impressions        INT,
        imp_total_ad_clicks             INT
);

grant select on &{pipeline_schema_sf}.user_hotel_advertiser_impressions_daily  to public;
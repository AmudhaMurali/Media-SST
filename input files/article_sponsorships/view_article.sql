create view if not exists &{pipeline_schema_sf}.view_article as

select

        ds                  ,
        url_id              ,
        op_advertiser_id    ,
        advertiser_name     ,
        order_id            ,
        sales_order_name    ,
        industry            ,
        advertiser_region   ,
        account_exec        ,
        article_title       ,
        sponsor_status      ,
        sponsor_name        ,
        use_sponsor_info    ,
        utm_source          ,
        locale              ,
        user_country_name   ,
        platform            ,

        marketing_campaign_id  ,
        campaign_start_date ,
        campaign_end_date   ,
        item_id             ,
        item_name           ,
        item_type           ,
        action_type         ,
        action_sub_type     ,

        null as shelf_type          ,
        null as shelf_imps          ,
        null as shelf_clicks        ,

        total_impression    ,
        total_users         ,
        total_interaction   ,

        null as uniques             ,
        null as total_dwell_time_ms ,
        null as avg_dwell_time_ms

from  &{pipeline_schema_sf}.article_enhance_inter a

union all

select
        ds                  ,
        url_id              ,
        op_advertiser_id    ,
        advertiser_name     ,
        order_id            ,
        sales_order_name    ,
        industry            ,
        advertiser_region   ,
        account_exec        ,
        article_title       ,
        sponsor_status      ,
        sponsor_name        ,
        use_sponsor_info    ,
        utm_source          ,
        locale              ,
        user_country_name   ,
        platform            ,
        marketing_campaign_id  ,

        null as campaign_start_date ,
        null as campaign_end_date   ,
        null as item_id             ,
        null as item_name           ,
        null as item_type           ,
        null as action_type         ,
        null as action_sub_type     ,

        null as shelf_type          ,
        null as shelf_imps          ,
        null as shelf_clicks        ,

        null as total_impression    ,
        null as total_users         ,
        null as total_interaction   ,

        uniques             ,
        total_dwell_time_ms ,
        avg_dwell_time_ms

from  &{pipeline_schema_sf}.sponsored_articles_dwell_time_agg_mcid t

union all

select
        ds                  ,
        url_id              ,
        op_advertiser_id    ,
        advertiser_name     ,
        order_id            ,
        sales_order_name    ,
        industry            ,
        advertiser_region   ,
        account_exec        ,
        article_title       ,
        sponsor_status      ,
        sponsor_name        ,
        use_sponsor_info    ,
        utm_source          ,
        locale              ,
        user_country_name   ,
        platform            ,

        marketing_campaign_id  ,
        campaign_start_date ,
        campaign_end_date   ,
        item_id             ,
        item_name           ,
        item_type           ,
        action_type         ,
        action_sub_type     ,

        shelf_type          ,
        shelf_imps          ,
        shelf_clicks        ,

        null as total_impression    ,
        null as total_users         ,
        null as total_interaction   ,

        null as uniques             ,
        null as total_dwell_time_ms ,
        null as avg_dwell_time_ms

from  &{pipeline_schema_sf}.article_shelf f

;

grant select on &{pipeline_schema_sf}.view_article to public;
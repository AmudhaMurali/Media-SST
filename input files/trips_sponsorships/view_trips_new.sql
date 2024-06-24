create view if not exists &{pipeline_schema}.view_trips_new as

select  ds                  ,
        trip_id             ,
        trip_title          ,
        username            ,
        display_name        ,
        locale              ,
        os_type             ,

       --op1
        op_advertiser_id    ,
        advertiser_name     ,
        order_id  as sales_order_id  ,
        sales_order_name    ,
        industry            ,
        region              ,
        account_exec        ,

       --pv
        user_country_id     ,
        user_country_name   , --dwell
        marketing_campaign_id  ,
        uniques             ,
        pageviews           ,

        -- dwell time
        null as page_name       ,
        null as servlet_name    ,
        null as url             ,
        null as url_rum         ,
        null as unique_id       ,
        null as dwell_time      ,
       null as dwell_time_uniques,

        -- poi
       null as detailid         ,
       null as detailid_name    ,
       null as user_id          ,
       null as poi_map_click    ,
       null as poi_carousel     ,
       null as poi_readmore     ,
       null as poi_clickthr     ,
       null as poi_map_clickthr ,
       null as poi_save         ,
       null as uniques_with_poi_actions,

       -- interaction
        null as trip_mapzoom        ,
        null as trip_readmore       ,
        null as trip_share          ,
        null as trip_save           ,
        null as trip_sponsor_click  ,
        null as uniques_with_interactions


from &{pipeline_schema}.sponsored_trips_pvs

union all

select  ds                  ,
        trip_id             ,
        trip_title          ,
        username            ,
        display_name        ,
        locale              ,
        os_type_name as os_type   ,

       --op1
        op_advertiser_id    ,
        advertiser_name     ,
        sales_order_id      ,
        sales_order_name    ,
        industry            ,
        region              ,
        account_exec        ,

       --pv
        null as user_country_id     ,
        user_country_name   , --dwell
        marketing_campaign_id  ,
        null as uniques             ,
        null as pageviews           ,

        -- dwell time
        page_name       ,
        servlet_name    ,
        url             ,
        url_rum         ,
        unique_id       ,
        dwell_time      ,
        1 as dwell_time_uniques,

        -- poi
       null as detailid         ,
       null as detailid_name    ,
       null as user_id          ,
       null as poi_map_click    ,
       null as poi_carousel     ,
       null as poi_readmore     ,
       null as poi_clickthr     ,
       null as poi_map_clickthr ,
       null as poi_save         ,
       null as uniques_with_poi_actions,

       -- interaction
        null as trip_mapzoom        ,
        null as trip_readmore       ,
        null as trip_share          ,
        null as trip_save           ,
        null as trip_sponsor_click  ,
        null as uniques_with_interactions

from &{pipeline_schema}.sponsored_trips_dwell_time_unique_sf

union all

select  ds                  ,
        tripid as trip_id   ,
        trip_title          ,
        username            ,
        null as display_name        ,
        locale              ,
        os_type             ,

       --op1
        op_advertiser_id    ,
        advertiser_name     ,
        sales_order_id      ,
        sales_order_name    ,
        industry            ,
        region              ,
        account_exec        ,

       --pv
        null as user_country_id     ,
        user_country_name   , --dwell
        marketing_campaign_id  ,
        null as uniques             ,
        null as pageviews           ,

        -- dwell time
        null as page_name       ,
        null as servlet_name    ,
        null as url             ,
        null as url_rum         ,
        null as unique_id       ,
        null as dwell_time      ,
        null as dwell_time_uniques,


        -- poi
        detailid         ,
        detailid_name    ,
        user_id          ,
        poi_map_click    ,
        poi_carousel     ,
        poi_readmore     ,
        poi_clickthr     ,
        poi_map_clickthr ,
        poi_save         ,
        uniques_with_poi_actions,

       -- interaction
        null as trip_mapzoom        ,
        null as trip_readmore       ,
        null as trip_share          ,
        null as trip_save           ,
        null as trip_sponsor_click  ,
        null as uniques_with_interactions

from &{pipeline_schema}.sponsored_trips_poi_agg

union all

select  ds                  ,
        trip_id             ,
        title as trip_title ,
        username            ,
        null as display_name,
        locale              ,
        os_type             ,

       --op1
        op_advertiser_id    ,
        advertiser_name     ,
        sales_order_id      ,
        sales_order_name    ,
        industry            ,
        region              ,
        account_exec        ,

       --pv
        null as user_country_id     ,
        null as user_country_name   , --dwell
        null as marketing_campaign_id  ,
        null as uniques             ,
        null as pageviews           ,

        -- dwell time
        null as page_name       ,
        null as servlet_name    ,
        null as url             ,
        null as url_rum         ,
        null as unique_id       ,
        null as dwell_time      ,
        null as dwell_time_uniques,

        -- poi
       null as detailid         ,
       null as detailid_name    ,
       null as user_id          ,
       null as poi_map_click    ,
       null as poi_carousel     ,
       null as poi_readmore     ,
       null as poi_clickthr     ,
       null as poi_map_clickthr ,
       null as poi_save         ,
       null as uniques_with_poi_actions,

       -- interaction
        trip_mapzoom        ,
        trip_readmore       ,
        trip_share          ,
        trip_save           ,
        trip_sponsor_click  ,
        uniques_with_interactions

from &{pipeline_schema}.sponsored_trips_interaction


;


grant select on &{pipeline_schema}.view_trips_new to public;
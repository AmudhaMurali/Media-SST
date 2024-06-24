create view if not exists &{pipeline_schema}.view_trips as

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
       null as clicks           ,
       null as likes            ,
       null as see_more         ,
       null as shares           ,
       null as maps             ,
       null as saves            ,
       null as uniques_with_poi_actions,
       null as uniques_with_poi_saves,

       -- social
       null as TRIP_LIKES   ,
       null as TRIP_REPOSTS ,
       null as TRIP_SHARES

from &{pipeline_schema}.sponsored_trips_pageviews

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
       null as clicks           ,
       null as likes            ,
       null as see_more         ,
       null as shares           ,
       null as maps             ,
       null as saves            ,
       null as uniques_with_poi_actions,
       null as uniques_with_poi_saves,

       -- social
       null as TRIP_LIKES   ,
       null as TRIP_REPOSTS ,
       null as TRIP_SHARES

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
       clicks           ,
       likes            ,
       see_more         ,
       shares           ,
       maps             ,
       saves            ,
       uniques_with_poi_actions,
       uniques_with_poi_saves,

       -- social
       null as TRIP_LIKES   ,
       null as TRIP_REPOSTS ,
       null as TRIP_SHARES

from &{pipeline_schema}.sponsored_trips_poi

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
       null as clicks           ,
       null as likes            ,
       null as see_more         ,
       null as shares           ,
       null as maps             ,
       null as saves            ,
       null as uniques_with_poi_actions,
       null as uniques_with_poi_saves,

       -- social
       TRIP_LIKES   ,
       TRIP_REPOSTS ,
       TRIP_SHARES

from &{pipeline_schema}.sponsored_trips_social

union all

select ds,
     trip_id,
     trip_title,
     username,
     display_name,
     locale,
     os_type_name as os_type,

     --op1
     op_advertiser_id,
     advertiser_name,
     sales_order_id,
     sales_order_name,
     industry,
     region,
     account_exec,

     --pv
     null         as user_country_id,
     user_country_name, --dwell
     marketing_campaign_id,
     null         as uniques,
     null         as pageviews,

     -- dwell time
     page_name,
     servlet_name,
     url,
     url_rum,
     unique_id,
     dwell_time,
     dwell_time_uniques,

     -- poi
     null         as detailid,
     null         as detailid_name,
     null         as user_id,
     null         as clicks,
     null         as likes,
     null         as see_more,
     null         as shares,
     null         as maps,
     null         as saves,
     null         as uniques_with_poi_actions,
     null         as uniques_with_poi_saves,

     -- social
     null         as TRIP_LIKES,
     null         as TRIP_REPOSTS,
     null         as TRIP_SHARES

from RIO_SF.CX_ANALYTICS.sponsored_trips_dwell_time_his

;

grant select on &{pipeline_schema}.view_trips to public;
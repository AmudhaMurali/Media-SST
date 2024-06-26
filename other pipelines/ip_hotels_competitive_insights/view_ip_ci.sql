create view if not exists &{pipeline_schema}.view_ip_ci as

select
        'listing'   as data_type,
        ds	                    ,
        brand_type	            ,
        brand	                ,
        brand_code	            ,
        parent_brand	        ,
        comp_for_country	    ,

        star_rating	            ,

        num_properties	        ,

        null as uniques                 ,
        null as pageviews               ,
        null as clicks                  ,
        null as est_bookings            ,
        null as bookings                ,
        null as avg_nights_per_booking  ,
        null as avg_nightly_spend       ,
        null as avg_num_guests          ,
        null as avg_num_rooms           ,
        null as avg_days_out            ,
        null as avg_properties_viewed_pp,
        null as num_property_metrics    ,
        null as uniques_pp              ,
        null as pageviews_pp            ,
        null as clicks_pp               ,
        null as est_bookings_pp         ,
        null as bookings_pp             ,

        null as management_responses	,
        null as review_count	        ,
        null as published_photo_count	,
        null as published_video_count	,
        null as avg_bubble_score	    ,
        null as rate_cleanliness	    ,
        null as rate_location	        ,
        null as rate_room	            ,
        null as rate_service	        ,
        null as rate_sleep	            ,
        null as rate_value	            ,
        null as num_property_ratings    ,
        null as management_responses_pp ,
        null as review_count_pp         ,
        null as published_photo_count_pp ,
        null as published_video_count_pp ,

        null as os_group                ,
        null as user_country_id         ,
        null as user_country_name       ,
        null as user_region_id          ,
        null as user_region_name        ,
        null as uniques_market

from &{pipeline_schema}.comp_ci_hotel_listings

union all

select  'metrics'   as data_type,
        ds	                    ,
        brand_type	            ,
        brand	                ,
        brand_code	            ,
        parent_brand	        ,
        comp_for_country	    ,

        null as star_rating	            ,

        null as num_properties	        ,

        uniques                 ,
        pageviews               ,
        clicks                  ,
        est_bookings            ,
        bookings                ,
        avg_nights_per_booking  ,
        avg_nightly_spend       ,
        avg_num_guests          ,
        avg_num_rooms           ,
        avg_days_out            ,
        avg_properties_viewed_pp,
        num_property_metrics    ,
        uniques_pp              ,
        pageviews_pp            ,
        clicks_pp               ,
        est_bookings_pp         ,
        bookings_pp             ,

        null as management_responses	,
        null as review_count	        ,
        null as published_photo_count	,
        null as published_video_count	,
        null as avg_bubble_score	    ,
        null as rate_cleanliness	    ,
        null as rate_location	        ,
        null as rate_room	            ,
        null as rate_service	        ,
        null as rate_sleep	            ,
        null as rate_value	            ,
        null as num_property_ratings    ,
        null as management_responses_pp ,
        null as review_count_pp         ,
        null as published_photo_count_pp ,
        null as published_video_count_pp ,

        null as os_group                ,
        null as user_country_id         ,
        null as user_country_name       ,
        null as user_region_id          ,
        null as user_region_name        ,
        null as uniques_market

from &{pipeline_schema}.comp_ci_hotel_metrics

union all

select  'ratings'   as data_type,
        ds	                    ,
        brand_type	            ,
        brand	                ,
        brand_code	            ,
        parent_brand	        ,
        comp_for_country	    ,

        star_rating	            ,

        null as num_properties	        ,

        null as uniques                 ,
        null as pageviews               ,
        null as clicks                  ,
        null as est_bookings            ,
        null as bookings                ,
        null as avg_nights_per_booking  ,
        null as avg_nightly_spend       ,
        null as avg_num_guests          ,
        null as avg_num_rooms           ,
        null as avg_days_out            ,
        null as avg_properties_viewed_pp,
        null as num_property_metrics    ,
        null as uniques_pp              ,
        null as pageviews_pp            ,
        null as clicks_pp               ,
        null as est_bookings_pp         ,
        null as bookings_pp             ,

        management_responses    ,
        review_count            ,
        published_photo_count   ,
        published_video_count   ,
        avg_bubble_score        ,
        rate_cleanliness        ,
        rate_location           ,
        rate_room               ,
        rate_service            ,
        rate_sleep              ,
        rate_value              ,
        num_property_ratings    ,
        management_responses_pp ,
        review_count_pp         ,
        published_photo_count_pp ,
        published_video_count_pp ,

        null as os_group                ,
        null as user_country_id         ,
        null as user_country_name       ,
        null as user_region_id          ,
        null as user_region_name        ,
        null as uniques_market

from &{pipeline_schema}.comp_ci_hotel_ratings

union all


select  'market'    as data_type,
        ds	                    ,
        brand_type	            ,
        brand	                ,
        brand_code	            ,
        parent_brand	        ,
        comp_for_country	    ,
--listings ratings
        null as star_rating	            ,
--listings
        null as num_properties	        ,
--metrics
        null as uniques                 ,
        null as pageviews               ,
        null as clicks                  ,
        null as est_bookings            ,
        null as bookings                ,
        null as avg_nights_per_booking  ,
        null as avg_nightly_spend       ,
        null as avg_num_guests          ,
        null as avg_num_rooms           ,
        null as avg_days_out            ,
        null as avg_properties_viewed_pp,
        null as num_property_metrics    ,
        null as uniques_pp              ,
        null as pageviews_pp            ,
        null as clicks_pp               ,
        null as est_bookings_pp         ,
        null as bookings_pp             ,
--ratings
        null as management_responses	,
        null as review_count	        ,
        null as published_photo_count	,
        null as published_video_count	,
        null as avg_bubble_score	    ,
        null as rate_cleanliness	    ,
        null as rate_location	        ,
        null as rate_room	            ,
        null as rate_service	        ,
        null as rate_sleep	            ,
        null as rate_value	            ,
        null as num_property_ratings    ,
        null as management_responses_pp ,
        null as review_count_pp         ,
        null as published_photo_count_pp ,
        null as published_video_count_pp ,
--market
        os_group                ,
        user_country_id         ,
        user_country_name       ,
        user_region_id          ,
        user_region_name        ,
        uniques as uniques_market


from &{pipeline_schema}.comp_ci_hotel_market
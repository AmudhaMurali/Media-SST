-- Top 50  source markets traffic (country, region, city) for each ad geo and across its competitive set in the past 90 days


begin;
delete from &{pipeline_schema}.ci_top_markets;

INSERT INTO &{pipeline_schema}.ci_top_markets
    select advertiser_id,
           advertiser_name,
           ad_name_formatted,
           ad_geo_id,
           geo_name ad_geo_name,
           ad_rank,
           'Country' as market_type,
           ad_user_country as market,
           null as market_in,
           ad_uniques,
           comp_rank,
           comp_user_country as comp_market,
           null as comp_market_in,
           comp_uniques
    from &{pipeline_schema}.ci_top_country_markets
union all
    select advertiser_id,
           advertiser_name,
           ad_name_formatted,
           ad_geo_id,
           geo_name as ad_geo_name,
           ad_rank,
           'Region' as market_type,
           ad_user_region as market,
           ad_country_of_reg as market_in,
           ad_uniques,
           comp_rank,
           comp_user_region as comp_market,
           comp_country_of_reg as comp_market_in,
           comp_uniques
    from &{pipeline_schema}.ci_top_region_markets
union all
    select advertiser_id,
           advertiser_name,
           ad_name_formatted,
           ad_geo_id,
           geo_name as ad_geo_name,
           ad_rank,
           'City' as market_type,
           ad_user_city as market,
           ad_reg_of_city as market_in,
           ad_uniques,
           comp_rank,
           comp_user_city as comp_market,
           comp_reg_of_city as comp_market_in,
           comp_uniques
    from &{pipeline_schema}.ci_top_city_markets
;

commit;
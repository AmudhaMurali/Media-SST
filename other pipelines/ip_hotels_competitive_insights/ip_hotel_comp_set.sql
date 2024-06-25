

begin;
delete from &{pipeline_schema}.ip_hotel_comp_set;

insert into &{pipeline_schema}.ip_hotel_comp_set

--to get competitive set for overall brand
select * from (
    select hex_encode(lower(loc_brand))     as brand_code,
           initcap(loc_brand)               as brand,
           loc_parent_brand                 as parent_brand,
           hex_encode(lower(pair_brand))    as comp_brand_code,
           initcap(pair_brand)              as comp_brand,
           pair_parent_brand                as comp_parent_brand,
           --to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
           'Overall Brand'                  as property_country_name,
           sum(score)                       as total_score,
           dense_rank() over (partition by initcap(loc_brand) order by total_score desc) as rank
    from display_ads.sales.competitive_brand_set_uc
    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
             'Overall Brand' --,to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end)
    )
where rank between '1' and '5'

union all

--to get competitive set for each country the brand has properties in
select * from (
    select hex_encode(lower(loc_brand))     as brand_code,
           initcap(loc_brand)               as brand,
           loc_parent_brand                 as parent_brand,
           hex_encode(lower(pair_brand))    as comp_brand_code,
           initcap(pair_brand)              as comp_brand,
           pair_parent_brand                as comp_parent_brand,
           --to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
           loc_country_name                 as property_country_name,
           sum(score)                       as total_score,
           dense_rank() over (partition by initcap(loc_brand), loc_country_name order by total_score desc) as rank
    from display_ads.sales.competitive_brand_set_uc
    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
             loc_country_name--,to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end),
    )
where rank between '1' and '5'

;

commit;

----to get score for overall brand, all property countries (per user country)
--select * from (
--    select hex_encode(lower(loc_brand))     as brand_code,
--           initcap(loc_brand)               as brand,
--           loc_parent_brand                 as parent_brand,
--           hex_encode(lower(pair_brand))    as comp_brand_code,
--           initcap(pair_brand)              as comp_brand,
--           pair_parent_brand                as comp_parent_brand,
--           to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
--           user_country_name,
--           'Overall Brand'                  as property_country_name,
--           sum(score)                       as total_score,
--           dense_rank() over (partition by initcap(loc_brand), user_country_name, same_parent_brand order by total_score desc) as rank
--    from display_ads.sales.competitive_brand_set_uc
--    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
--             to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end), user_country_name, 'Overall Brand'
--    )
--where rank between '1' and '15'--

--union all--

----to get score for overall users, all user countries (per property country)
--select * from (
--    select hex_encode(lower(loc_brand))     as brand_code,
--           initcap(loc_brand)               as brand,
--           loc_parent_brand                 as parent_brand,
--           hex_encode(lower(pair_brand))    as comp_brand_code,
--           initcap(pair_brand)              as comp_brand,
--           pair_parent_brand                as comp_parent_brand,
--           to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
--           'All'                            as user_country_name,
--           loc_country_name                 as property_country_name,
--           sum(score)                       as total_score,
--           dense_rank() over (partition by initcap(loc_brand), loc_country_name, same_parent_brand order by total_score desc) as rank
--    from display_ads.sales.competitive_brand_set_uc
--    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
--             to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end), 'All', loc_country_name
--    )
--where rank between '1' and '15'--

--union all--

----all intersection of user country and property country
--select * from (
--    select hex_encode(lower(loc_brand))     as brand_code,
--           initcap(loc_brand)               as brand,
--           loc_parent_brand                 as parent_brand,
--           hex_encode(lower(pair_brand))    as comp_brand_code,
--           initcap(pair_brand)              as comp_brand,
--           pair_parent_brand                as comp_parent_brand,
--           to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
--           user_country_name,
--           loc_country_name                 as property_country_name,
--           sum(score)                       as total_score,
--           dense_rank() over (partition by initcap(loc_brand), user_country_name, loc_country_name, same_parent_brand order by total_score desc) as rank
--    from display_ads.sales.competitive_brand_set_uc
--    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
--             to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end), user_country_name, loc_country_name
--    )
--where rank between '1' and '15'--

--union all--

----ranking for total brand property locations across all user countries
--select * from (
--    select hex_encode(lower(loc_brand))     as brand_code,
--           initcap(loc_brand)               as brand,
--           loc_parent_brand                 as parent_brand,
--           hex_encode(lower(pair_brand))    as comp_brand_code,
--           initcap(pair_brand)              as comp_brand,
--           pair_parent_brand                as comp_parent_brand,
--           to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end) as same_parent_brand,
--           'All'                            as user_country_name,
--           'Overall Brand'                  as property_country_name,
--           sum(score)                       as total_score,
--           dense_rank() over (partition by initcap(loc_brand), same_parent_brand order by total_score desc) as rank
--    from display_ads.sales.competitive_brand_set_uc
--    group by hex_encode(lower(loc_brand)), initcap(loc_brand), loc_parent_brand, hex_encode(lower(pair_brand)), initcap(pair_brand), pair_parent_brand,
--             to_boolean(case when loc_parent_brand = pair_parent_brand then 1 else 0 end), 'All', 'Overall Brand'
--    )
--where rank between '1' and '15'
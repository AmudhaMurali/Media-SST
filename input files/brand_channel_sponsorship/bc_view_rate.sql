begin;
delete from &{pipeline_schema_sf}.bc_view_rate
where ds BETWEEN '&{start_dt}' AND '&{end_dt}';


insert into &{pipeline_schema_sf}.bc_view_rate

with user as (
    select ds,
           ta_unique_id,
           GEO_ID                                               as bc_geo_id,
           SPONSOR_NAME                                         as bc_geo_name,
           url,
           marketing_campaign_id,
           USER_COUNTRY_ID,
           user_country_name,
           pv.locale,
           CLIENT_TYPE,
           case when SPONSOR_NAME is not null then 1 else 0 end as saw_brand_channel_page,
           --sum(retention_users) as retention_users,
           count(*)                                           as pageviews
    from USER_TRACKING.PUBLIC.BLESSED_PAGEVIEWS pv
             left join (select distinct CAMPAIGN_START_DATE, CAMPAIGN_END_DATE, SPONSOR_NAME, GEO_ID
                        from DISPLAY_ADS_ENGINEERING.CONTENT_SPONSORSHIPS.SPOCK_SPONSORSHIP_CAMPAIGNS
                        where CAMPAIGN_TYPE = 'Brand Channel') c
                       on geo_id = geo_location_id and ds >= campaign_start_date and ds <= campaign_end_date
    where ds BETWEEN '&{start_dt}' AND '&{end_dt}'
      and page_name = 'Tourism'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

select     ds,
           bc_geo_id,
           bc_geo_name,
           url,
           marketing_campaign_id,
           USER_COUNTRY_ID,
           user_country_name,
           locale,
           CLIENT_TYPE,
           saw_brand_channel_page,
           count(distinct ta_unique_id) as unique_users,
           sum(case when pageviews >1 then 1 else 0 end ) as retention_users
from user
group by 1,2,3,4,5,6,7,8,9,10

;

commit;


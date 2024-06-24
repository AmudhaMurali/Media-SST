
begin;
delete from &{pipeline_schema}.trips_orderid_mapping
where ds between '&{start_dt}' and '&{end_dt}';

insert into &{pipeline_schema}.trips_orderid_mapping
select  DISTINCT ds,cast( id.ITEM_ID as int ) as trip_id, case when cast(regexp_substr(custom_data,'[0-9]+') as int) = 0 OR cast(regexp_substr(custom_data,'[0-9]+') as int) IS NULL then map.order_id
        else cast(regexp_substr(custom_data,'[0-9]+') as int) end as order_id
from
    (select distinct cast(ITEM_ID AS bigint) AS ITEM_ID from
    (select distinct try_cast(ITEM_ID AS bigint) ITEM_ID from user_tracking.public.user_impressions where ds between '&{start_dt}' and '&{end_dt}' and ITEM_TYPE = 'tripSponsorLogoImpression'
    union all
    select distinct  trip_id from  display_ads.sales.historical_trips) ) id

left join  display_ads.sales.historical_trips map on cast( id.ITEM_ID as string ) =  cast( map.trip_id as string )
left join (select distinct ds, ITEM_ID, CUSTOM_DATA from
            (select distinct ds, try_cast(ITEM_ID AS bigint) ITEM_ID, CUSTOM_DATA,
                             row_number() over(partition by ITEM_ID order by ds desc,cast(regexp_substr(custom_data,'[0-9]+') as int) desc) as rank
             from user_tracking.public.user_impressions where ds between '&{start_dt}' and '&{end_dt}' and ITEM_TYPE = 'tripSponsorLogoImpression')
             where rank = 1) imp
on cast( id.ITEM_ID as string ) =  cast( imp.ITEM_ID as string )

;

commit;

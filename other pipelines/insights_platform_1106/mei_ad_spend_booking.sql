
-- create table joining together accommodation and attractions bookings
-- with DFP ad spend


begin;

delete from &{pipeline_schema}.mei_ad_spend_booking
where ds = '&{start_dt}';

insert into &{pipeline_schema}.mei_ad_spend_booking (
    DS                      ,
    BILLING_PERIOD_NAME     ,
    ADVERTISER_ID           ,
    ADVERTISER_NAME         ,
    AD_NAME_FORMATTED       ,
    UNIQUES                 ,
    ACC_BOOKINGS            ,
    ATTR_BOOKINGS           ,
    RECOGNIZED_REVENUE
)
SELECT date_trunc('MONTH',ca.ds)        AS DS,
       a.BILLING_PERIOD_NAME            AS BILLING_PERIOD_NAME,
       ca.ADVERTISER_ID                 AS ADVERTISER_ID,
       adname.ADVERTISER_NAME           AS ADVERTISER_NAME,
       adname.AD_NAME_FORMATTED         AS AD_NAME_FORMATTED,
       sum(ca.UNIQUES_COUNT)            AS UNIQUES,
       sum(ca.HOTEL_BOOKINGS)           AS ACC_BOOKINGS,
       sum(ca.TOTAL_ATTR_BOOKINGS)      AS ATTR_BOOKINGS,
       a.RECOGNIZED_REVENUE             AS RECOGNIZED_REVENUE
FROM &{pipeline_schema}.mei_campaign_agg ca
LEFT JOIN &{pipeline_schema}.mei_advertiser_spend a on a.billing_period_start = date_trunc('MONTH',ca.ds) and a.advertiser_id = ca.advertiser_id
LEFT JOIN &{pipeline_schema}.dfp_ad_name adname on ca.advertiser_id = adname.advertiser_id
WHERE ca.ds = '&{start_dt}'
GROUP BY date_trunc('MONTH',ca.ds),
         a.BILLING_PERIOD_NAME,
         ca.ADVERTISER_ID,
         adname.ADVERTISER_NAME,
         adname.AD_NAME_FORMATTED,
         a.RECOGNIZED_REVENUE
;

commit;

-- test table user_scratch.x_arosenthal.mei_ad_spend_booking_temp
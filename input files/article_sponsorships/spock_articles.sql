DELETE FROM &{pipeline_schema_sf}.article_sponsored_profile_key;
INSERT into &{pipeline_schema_sf}.article_sponsored_profile_key
SELECT a.id as article_id,
       a.campaign_id,
       a.operative_order_id as order_id,
       a.url,
       a.locale,
       c.start_date as campaign_start_date,
       c.end_date as campaign_end_date,
       c.sponsor_id,
       s.name as sponsor_name,
       CONCAT('[', start_date, ',', end_date, ')') as dates
FROM &{pipeline_schema_sf}.articles a
        LEFT JOIN &{pipeline_schema_sf}.campaigns c ON a.campaign_id = c.id --and c.start_date <= CURRENT_DATE and c.end_date >= CURRENT_DATE
        LEFT JOIN &{pipeline_schema_sf}.sponsors s on c.sponsor_id = s.id


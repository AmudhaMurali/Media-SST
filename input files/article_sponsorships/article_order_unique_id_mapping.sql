-- Creating a mapping table with distinct order mapping


BEGIN;
DELETE FROM &{pipeline_schema_sf}.article_order_unique_id_mapping;
;

INSERT INTO &{pipeline_schema_sf}.article_order_unique_id_mapping
SELECT
    ORDER_ID
    , URL_ID
FROM
    (SELECT
        ORDER_ID
        , URL_ID
        -- trying to dedupe based on date. some level of randomness if more than 1 order per day:
        , ROW_NUMBER () OVER (PARTITION BY URL_ID ORDER BY DS desc, ORDER_ID desc) as dedupe
    FROM display_ads.sales.article_order_id_mapping)
WHERE dedupe = 1
  AND URL_ID IS NOT NULL
;

commit;

-- -- this table maps advertiser id to advertiser name from operative one
--
-- begin;
-- delete from &{pipeline_schema}.dfp_ad_name;
--
-- INSERT INTO   &{pipeline_schema}.dfp_ad_name (
--   	advertiser_id,
--   	advertiser_name,
--   	ad_name_formatted
--   	)
-- SELECT DISTINCT advertiser_id                                   AS advertiser_id,
--                 advertiser_name                                 AS advertiser_name,
--                 replace(substr(ADVERTISER_NAME,4,100),'_', ' ') AS ad_name_formatted
-- FROM display_ads.operative_one.publisher_system_ad
-- WHERE production_system_name = 'DFP Primary'
-- ;

--------------
-- this table maps advertiser id to advertiser name from operative one

begin;
delete from &{pipeline_schema}.dfp_ad_name;

INSERT INTO   &{pipeline_schema}.dfp_ad_name (
  	advertiser_id,
  	advertiser_name,
  	ad_name_formatted
  	)

select distinct advertiser_id, max(advertiser_name),max(ad_name_formatted)
from
(SELECT DISTINCT cast(advertiser_id as int)                         AS advertiser_id,
                 advertiser_name                                    AS advertiser_name,
                 replace(substr(ADVERTISER_NAME, 4, 100), '_', ' ') AS ad_name_formatted
 FROM display_ads.operative_one.publisher_system_ad
 WHERE production_system_name = 'DFP Primary'

 union

 SELECT DISTINCT cast(advertiser_ad_server_id as int)               AS advertiser_id,
                 advertiser_name                                    AS advertiser_name,
                 replace(substr(advertiser_name, 4, 100), '_', ' ') AS ad_name_formatted
 FROM DISPLAY_ADS.PIO.PIO_OP1_DATA_SHIM
 WHERE line_item_ad_server='dfp'--not sure if this is right field - needs to confirm with Bridget
)
group by 1
;

commit;
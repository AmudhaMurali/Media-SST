-- this table maps advertiser id to advertiser name from operative one

begin;
delete from &{pipeline_schema}.dfp_pio_ad_name;

INSERT INTO   &{pipeline_schema}.dfp_pio_ad_name (
  	advertiser_id,
  	advertiser_name,
  	ad_name_formatted
  	)
SELECT DISTINCT advertiser_ad_server_id                         AS advertiser_id,
                advertiser_name                                 AS advertiser_name,
                replace(substr(advertiser_name,4,100),'_', ' ') AS ad_name_formatted
FROM user_scratch.x_cpilz.pio_op1_data_shim
WHERE campaign_delivery_status = 'active' --not sure if this is right field - needs to confirm with Bridget
;


commit;





--------------------------------------------- OP1 LINE ITEMS ---------------------------------------------------

INSERT INTO &{pipeline_schema_sf}.base_op1_line_items
SELECT
    OP_ADVERTISER_ID,
    ADVERTISER_NAME,
    SALES_ORDER_ID,
    SALES_ORDER_NAME,
    INDUSTRY,
    ACCOUNT_EXEC
FROM DISPLAY_ADS.SALES.OP1_LINE_ITEMS ;

-------------------------------------------- OP1 DATA SHIM ----------------------------------------------------
INSERT INTO &{pipeline_schema_sf}.base_op1_data_shim
SELECT
    ADVERTISER_PIO AS OP_ADVERTISER_ID,
    ADVERTISER_NAME,
    CAMPAIGN_NAME AS SALES_ORDER_NAME,
    INDUSTRY,
    OWNER_NAME AS ACCOUNT_EXEC

FROM DISPLAY_ADS.PIO.PIO_OP1_DATA_SHIM ;




------------------------------------------------------------------------------------------------
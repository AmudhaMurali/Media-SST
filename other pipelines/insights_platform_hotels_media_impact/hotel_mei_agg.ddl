
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.hotel_mei_agg (
    DS                                  DATE    ,
    OS_TYPE                             STRING  ,
    OS_GROUP                            STRING  ,
    LOCALE                              STRING  ,
    USER_COUNTRY                        STRING  ,
    USER_CONTINENT                      STRING  ,
    USER_STATE                          STRING  ,
    USER_MARKET                         STRING  ,
    ADVERTISER_ID                       INT  ,
    ADVERTISER_NAME                     STRING  ,
    AD_NAME_FORMATTED                   STRING  ,
    AD_BRAND_NAME                       STRING ,
    AD_PARENT_BRAND_NAME                STRING ,
    SALES_REGION                        STRING ,
    ADVERTISER_CATEGORY                 STRING  ,
    IS_LOOKER                           BOOLEAN ,
    NUM_LOOKERS                         INT  ,
    UNIQUES_COUNT                       INT  ,
    SAW_CAMPAIGN                        BOOLEAN ,
    IMP_DS                              DATE    ,
    DAYDIF                              INT  ,
    NUM_SAW_CAMPAIGN                    INT  ,
    UU_W_INTERACTIONS                   INT  ,
    USER_CLICKED_A_LOCATION             BOOLEAN ,
    NUM_ACC_BOOKERS                     INT  ,
    USER_BOOKED_A_LOCATION              BOOLEAN ,

    CLICK_COUNT                         INT  ,
    CLICK_COUNT_DISTINCT                INT  ,
    PV_COUNT                            INT  ,
    PV_COUNT_DISTINCT                   INT  ,
    HOTEL_ESTIMATED_BOOKINGS            double  ,
    HOTEL_ESTIMATED_BOOKINGS_DISTINCT   INT  ,

    TOTAL_NIGHTS_BOOKED                 INT ,
    AVG_NIGHTLY_BOOKING_SPEND           double ,
    AVG_NUM_BOOKING_GUESTS              double ,
    AVG_NUM_BOOKING_ROOMS               double ,
    AVG_DAILY_RATE                      double
);

GRANT SELECT ON &{pipeline_schema_sf}.hotel_mei_agg TO PUBLIC;
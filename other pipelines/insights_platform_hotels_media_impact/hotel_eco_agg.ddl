
CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.hotel_eco_agg (
    DS                                  DATE    ,
    OS_TYPE                             STRING  ,
    OS_GROUP                            STRING  ,
    LOCALE                              STRING  ,
    USER_COUNTRY                        STRING  ,
    USER_CONTINENT                      STRING  ,
    USER_STATE                          STRING  ,
    USER_MARKET                         STRING  ,
    ADVERTISER_ID                       INT     ,
    ADVERTISER_NAME                     STRING  ,
    AD_NAME_FORMATTED                   STRING  ,
    AD_BRAND_NAME                       STRING ,
    AD_PARENT_BRAND_NAME                STRING ,
    HOTEL_COUNTRY                       STRING ,
    SALES_REGION                        STRING ,
    ADVERTISER_CATEGORY                 STRING  ,
    TRAVELER_TYPE                       STRING ,

    SAW_CAMPAIGN                        BOOLEAN ,
    IMP_DS                              DATE    ,
    DAYDIF                              INT  ,
    USER_BOOKED_A_LOCATION              BOOLEAN ,

    UNIQUES_COUNT                       INT  ,
    HOTEL_ESTIMATED_BOOKINGS            double  ,
    HOTEL_ESTIMATED_BOOKINGS_DISTINCT   INT  ,
    TOTAL_NIGHTS_BOOKED                 INT ,
    AVG_NIGHTLY_BOOKING_SPEND           double ,
    AVG_NUM_BOOKING_GUESTS              double ,
    AVG_NUM_BOOKING_ROOMS               double ,
    AVG_DAILY_RATE                      double ,
    AVG_ACCOMODATION_DAYS_OUT           double,
    CLICK_COUNT                         INT,
    SPLIT_COLUMN                        STRING

);

GRANT SELECT ON &{pipeline_schema_sf}.hotel_eco_agg TO PUBLIC;
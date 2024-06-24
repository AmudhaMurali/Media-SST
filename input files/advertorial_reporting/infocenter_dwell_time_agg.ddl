CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.infocenter_dwell_time_agg (
        DS                  date,
        SERVLET_NAME        string,
        DISPLAY_NAME        string,
        OP_ADVERTISER_ID    int,
        ADVERTISER_NAME     string,
        ORDER_ID            int,
        SALES_ORDER_NAME    string,
        INDUSTRY            string,
        ADVERTISER_REGION   string,
        ACCOUNT_EXEC        string,
        UTM_SOURCE          string,
        LOCALE              string,
        USER_COUNTRY_NAME   string,
        OS_TYPE_NAME        string,
        UNIQUES             int,
        TOTAL_DWELL_TIME    double,
        AVG_DWELL_TIME      double,
        mcid_tran           string

);

GRANT SELECT ON &{pipeline_schema_sf}.infocenter_dwell_time_agg TO PUBLIC;
CREATE TABLE IF NOT EXISTS &{pipeline_schema}.mei_advertiser_spend (
        DS                          date,
        MONTH_AND_YEAR              date,
        OP_ADVERTISER_ID            int,
        OP_ADVERTISER_NAME          string,
        AD_SERVER                   string,
        AD_SERVER_ADVERTISER_ID     int,
        AD_SERVER_ADVERTISER_NAME   string,
        AD_NAME_FORMATTED           string,
        CATEGORY                    string,
        FORECAST_CATEGORY           string,
        SALES_ORDER_ID              int,
        SALES_ORDER_NAME            string,
        SALES_ORDER_LINE_ITEM_ID    int,
        SALES_ORDER_LINE_ITEM_NAME  string,
        LINE_ITEM_START_DATE        date,
        LINE_ITEM_END_DATE          date,
        CURRENCY_LOCAL_REV          string,
        LOCAL_REVENUE               double,
        USD_REVENUE                 double,
        CURRENCY_LOCAL_CONTRACT     string,
        LOCAL_CONTRACTED_AMOUNT     double,
        USD_CONTRACTED_AMOUNT       double
);

GRANT SELECT ON &{pipeline_schema}.mei_advertiser_spend TO PUBLIC;
--update
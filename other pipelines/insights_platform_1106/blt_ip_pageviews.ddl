CREATE TABLE IF NOT EXISTS &{pipeline_schema}.blt_ip_pageviews (
                DS                      DATE,
                GEO_ID                  INT,
                GEO_NAME                STRING,
                IN_COUNTRY              STRING,
                IN_REG                  STRING,
                GEO_PLACETYPE_NAME      STRING,
                USER_COUNTRY_ID         INT,
                USER_COUNTRY_NAME       STRING,
                UNIQUES                 INT,
                PAGE_VIEWS              INT
);

GRANT ALL ON &{pipeline_schema}.blt_ip_pageviews TO PUBLIC;
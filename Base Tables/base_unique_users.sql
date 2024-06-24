DELETE FROM &{pipeline_schema}.base_unique_users WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.base_unique_users
SELECT
    DS,
    UNIQUE_ID,
    COMMERCE_COUNTRY_ID,
    USER_COUNTRY_NAME

FROM RIO_SF.rust.a_unique_users;
--CREATE SCHEMA IF NOT EXISTS &{pipeline_schema_sf};

CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.infocenter_dwell_time_by_unique
(
    page_name               STRING,
    servlet_name            STRING,
    page_id                 STRING,
    locale                  STRING,
    page_action             STRING,
    url                     STRING,
    url_rum                 STRING,
    user_country_name       STRING,
    os_type_name            STRING,
    unique_id               STRING,
    dwell_time                 INT,
    ds                        date,
    mcid                    String
);

GRANT SELECT ON &{pipeline_schema_sf}.infocenter_dwell_time_by_unique TO PUBLIC;

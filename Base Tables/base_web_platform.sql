DELETE FROM &{pipeline_schema}.base_rum_page_loads WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';
DELETE FROM &{pipeline_schema}.base_rum_metrics WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';
DELETE FROM &{pipeline_schema}.base_rum_navigations WHERE DS BETWEEN '&{start_dt}' and '&{end_dt}';

INSERT INTO &{pipeline_schema}.base_rum_page_loads
SELECT
    DS

FROM web_platform.public.rum_page_loads;

------------------------------------------------------------------------------------------------
INSERT INTO &{pipeline_schema}.base_rum_metrics
SELECT
    DS,
    value_ as dwell_time
FROM web_platform.public.rum_metrics;



------------------------------------------------------------------------------------------------
INSERT INTO &{pipeline_schema}.base_rum_navigations
SELECT
    DS,


FROM web_platform.public.rum_navigations;
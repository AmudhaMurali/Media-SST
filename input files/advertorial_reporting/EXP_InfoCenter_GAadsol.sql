INSERT OVERWRITE TABLE ${pipeline_schema}.Infocenter_GAadsol PARTITION(ds)

SELECT
    sol.locale
    ,sol.device_class
    ,sol.display_name
    ,sol.page_action
    ,COUNT(1) AS action_count
    ,COUNT(DISTINCT uuid) AS unique_users
    ,sol.ds
FROM (
    SELECT
        ga.ds
        ,ga.client_type
        ,ga.device_class
        ,COALESCE(device_id, ta_persistentcookie) AS uuid
        ,locale
        ,ga.page_properties AS page_properties
        ,regexp_extract(page_properties, 'InfoCenterV6_(.*)', 1) as display_name
        ,ga.page_action AS page_action
        ,ga.url
        ,ga.gass
    FROM sales.a_adsol_garecord ga
    LEFT JOIN t_location loc ON loc.id = ga.gasl
      WHERE ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND ds <= '${hiveconf:end_dt}'
        AND servlet_name = 'GARecord'
        AND ga.page_properties LIKE 'InfoCenterV6%'
        AND ga.gass = 'InfoCenterV6'
    ) sol
GROUP BY
    sol.locale
    ,sol.device_class
    ,sol.display_name
    ,sol.page_action
    ,sol.ds


-- Old advertorial dwell time report. Decommissioned as of 7/7/2021. Replaced with RUM reporting in ${pipeline_schema}InfoCenter_Dwell_Time_By_Unique

INSERT OVERWRITE TABLE ${pipeline_schema}.Infocenter_dwell_report PARTITION(ds)

SELECT
  pvuu.locale,
  pvuu.page_action as display_name,
  marketing_campaign_id,
  device_class,
  ip_country,
  COALESCE(dwell.dwell_time, 0) as dwell_time,
  COALESCE(dwell.samples_total, cast(0 as bigint)) as dwell_samples_total,
  COALESCE(dwell.samples_10_to_15min, cast(0 as bigint)) as dwell_samples_10_to_15min,
  dwell.ds
FROM (
     SELECT
        ds,
        locale,
        page_action,
        marketing_campaign_id,
        device_class,
        ip_country,
        sum(pageviews) as pageviews,
        count(uuid) AS unique_users
      from (
        select
          ds,
          locale,
          page_action,
          marketing_campaign_id,
          device_class,
          ip_country,
          coalesce(device_id, ta_persistentcookie) as uuid,
          count(1) AS pageviews
        from sales.a_adsol_lookback
        where ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND ds <= '${hiveconf:end_dt}'
          and is_real_pv = 1
          and servlet_name = 'InfoCenterV6'
        group by
          ds,
          locale,
          page_action,
          coalesce(device_id, ta_persistentcookie),
          marketing_campaign_id,
          device_class,
          ip_country
        ) subq
      group by
        ds,
        locale,
        page_action,
        marketing_campaign_id,
        device_class,
        ip_country
    ) pvuu
LEFT OUTER JOIN (
    SELECT
        maxTime.ds,
        maxTime.locale,
        maxTime.display_name,
        maxTime.dwell_time,
        COUNT(1) as samples_total,
        SUM (IF (maxTime.dwell_time >= 600 and maxTime.dwell_time < 900, 1, 0)) as samples_10_to_15min
    FROM (
        SELECT
            ds,
            locale,
            COALESCE(device_id, ta_persistentcookie) as unique_id,
            COALESCE( regexp_extract(page_action, 'dwell_InfoCenterV6_(.*)', 1), "UNKNOWN") as display_name,
            puid as page_id,
            COALESCE(MAX(dwell_time), 0) AS dwell_time
        FROM sales.a_adsol_viewability
        WHERE ds >= DATE_SUB('${hiveconf:start_dt}', 2) AND ds <= '${hiveconf:end_dt}'
            AND page_action like 'dwell_InfoCenterV6%'
            AND product_attr regexp 'inView_[0-9]+s'
            AND gass = 'InfoCenterV6'
        GROUP BY ds,
            locale,
            COALESCE(device_id, ta_persistentcookie),
            COALESCE( regexp_extract(page_action, 'dwell_InfoCenterV6_(.*)', 1), "UNKNOWN"),
            puid
        ) maxTime
    GROUP BY
        maxTime.ds,
        maxTime.locale,
        maxTime.display_name,
        maxTime.dwell_time
    ) dwell ON dwell.ds = pvuu.ds
        AND dwell.locale = pvuu.locale AND dwell.display_name = pvuu.page_action


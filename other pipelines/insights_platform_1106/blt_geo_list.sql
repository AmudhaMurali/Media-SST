--- this is the table to add new geo_ids into when we want
--- them to be included in the baseline trends dashboard.
--- currently, this includes all advertisers that spent
--- $250K+ in 2018. Once added to this list, the tables
--- will need to be backfilled again to get historical data.

begin;
delete from &{pipeline_schema}.blt_geo_list;

INSERT INTO &{pipeline_schema}.blt_geo_list
    SELECT DISTINCT g.geo_id
    FROM display_ads.sales.gdoc_baseline_trends_geos g
    ;

commit;
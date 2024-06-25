CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_bubble_score (
                DS              DATE,
                GEO_ID          INT,
                GEO_NAME        STRING,
                PLACETYPE       STRING,
                NUM_LOCATIONS   INT,
                W_AVG_SCORE     INT,
                NUM_REVIEWS     INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_bubble_score TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_bubble_score TO PUBLIC;

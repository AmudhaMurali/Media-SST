CREATE TABLE IF NOT EXISTS &{pipeline_schema}.ci_comp_bubble_score (
                DS                      DATE,
                AD_GEO                  INT,
                AD_GEO_NAME             STRING,
                ADVERTISER_ID           INT,
                ADVERTISER_NAME         STRING,
  	            AD_NAME_FORMATTED       STRING,
  	            ADVERTISER_CATEGORY     STRING,
                AD_GEO_PLACETYPE        STRING,
                AD_GEO_NUM_LOC          INT,
                AD_GEO_BUBBLE_SCORE     INT,
                AD_GEO_NUM_REVIEWS      INT,
                COMP_RANK               INT,
                SIMILAR_GEO             INT,
                SIM_GEO_NAME            STRING,
                SIM_GEO_STATE           STRING,
                SIM_GEO_PLACETYPE       STRING,
                SIM_GEO_NUM_LOC         INT,
                SIM_GEO_BUBBLE_SCORE    INT,
                SIM_GEO_NUM_REVIEWS     INT
);

GRANT OWNERSHIP ON TABLE &{pipeline_schema}.ci_comp_bubble_score TO ROLE cx_analytics_role REVOKE CURRENT GRANTS;
GRANT SELECT ON &{pipeline_schema}.ci_comp_bubble_score TO PUBLIC;
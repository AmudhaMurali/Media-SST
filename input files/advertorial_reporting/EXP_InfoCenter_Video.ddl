-- This is currently unused in reporting

CREATE SCHEMA IF NOT EXISTS ${pipeline_schema};

CREATE TABLE IF NOT EXISTS ${pipeline_schema}.Infocenter_video
(
    id INT,
    display_name STRING,
    title STRING,
    type STRING,
    video_duration_seconds DECIMAL(19,9),
    time_played_seconds DECIMAL(19,9),
    max_played_time_seconds DECIMAL(19,9),
    played_to_end BOOLEAN,
    max_progress_percentage INT,
    locale STRING

)
PARTITIONED BY (ds STRING)
STORED AS ORC;
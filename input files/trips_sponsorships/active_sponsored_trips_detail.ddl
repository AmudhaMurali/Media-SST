

CREATE TABLE IF NOT EXISTS &{pipeline_schema}.active_sponsored_trips_detail (

    user_id         string,
    username        string,
    display_name    string,
    trip_id         int,
    trip_title      string,
    trip_desc       string,
    created         date,
    first_published date

);

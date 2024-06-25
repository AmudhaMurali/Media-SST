CREATE TABLE IF NOT EXISTS &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily (
    ds			                            date,
    unique_id			                    string,
    hotel_country                           string,
    advertiser_id			                integer,


    click_count                             integer,
    click_count_distinct                    integer,
    --min_click_timestamp                     string,
    --max_click_timestamp                     string,

    hotel_estimated_bookings                double,
    hotel_estimated_bookings_distinct       integer,
    --min_booking_timestamp                   string,
    --max_booking_timestamp                   string,

    pv_count                                integer,
    pv_count_distinct                       integer,
    --min_pv_timestamp                        string,
    --max_pv_timestamp                        string,

    --accomodation_click_count                integer,
    --accomodation_pv_count                   integer,
    --num_distinct_accomodations_clicked      integer,
    --num_distinct_accomodations_viewed       integer,

    total_nights_booked                     integer,
    avg_nightly_booking_spend               number(16,2),
    --avg_nightly_click_spend                 number(16,2),
    avg_num_booking_guests                  number(16,2),
    --avg_num_click_guests                    number(16,2),
    avg_num_booking_rooms                   number(16,2),
    --avg_num_click_rooms                     number(16,2),
    avg_accomodation_days_out               number(16,2),
    avg_daily_rate                          number(16,2)
);

grant select on &{pipeline_schema_sf}.user_hotel_advertiser_interactions_daily  to public;

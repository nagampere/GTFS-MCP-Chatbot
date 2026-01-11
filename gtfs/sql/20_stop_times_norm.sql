-- GTFS SQL models for MotherDuck / DuckDB
-- Generated: 2026-01-08
--
-- Assumptions:
-- - Raw GTFS tables exist as: agency, stops, routes, trips, stop_times, calendar, calendar_dates, shapes, frequencies, transfers
-- - stop_times.arrival_time / departure_time are "seconds since midnight" (can exceed 86400 for after-midnight service)
-- - calendar.start_date / end_date are DATE
--
-- Tips:
-- - These files create views in schema `gtfs`. If you prefer another schema, search/replace `gtfs.`.
-- - If your raw tables live in another schema, qualify them (e.g., raw.calendar).
create schema if not exists gtfs;

-- Normalizes stop_times by turning seconds-since-midnight into timestamps relative to service_date.
-- Notes:
-- - Times may exceed 86400 (24h) for after-midnight trips; this view handles that naturally via interval arithmetic.
create or replace view gtfs.stop_times_norm as
select
  st.trip_id,
  st.stop_id,
  st.stop_sequence,
  st.arrival_time,
  st.departure_time,
  -- arrival/departure as timestamps relative to service day
  (sd.service_date::timestamp + st.arrival_time * interval 1 second)   as arrival_ts,
  (sd.service_date::timestamp + st.departure_time * interval 1 second) as departure_ts,
  sd.service_date,
  -- convenience: day_offset in days (useful for debugging after-midnight)
  floor(st.departure_time / 86400) as departure_day_offset
from stop_times st
join trips t on st.trip_id = t.trip_id
join gtfs.service_days sd on t.service_id = sd.service_id;

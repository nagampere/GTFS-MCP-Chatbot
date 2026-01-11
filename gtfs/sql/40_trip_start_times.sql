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

-- Trip "start" time defined as the first stop_sequence for each trip & service_date.
-- Useful for headway estimation by route/direction.
create or replace view gtfs.trip_start_times as
with first_stop as (
  select
    trip_id,
    service_date,
    min(stop_sequence) as first_seq
  from gtfs.stop_times_norm
  group by 1,2
)
select
  n.trip_id,
  n.service_date,
  n.stop_id as start_stop_id,
  n.departure_ts as start_departure_ts
from gtfs.stop_times_norm n
join first_stop f
  on n.trip_id = f.trip_id
 and n.service_date = f.service_date
 and n.stop_sequence = f.first_seq;

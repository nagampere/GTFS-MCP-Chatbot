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

-- A convenient departure fact table for dashboards: one row per (stop_id, departure_ts, trip)
create or replace view gtfs.departures as
select
  n.stop_id,
  n.departure_ts,
  n.service_date,
  n.trip_id,
  t.route_id,
  t.direction_id,
  t.shape_id,
  r.agency_id,
  r.route_short_name,
  r.route_long_name,
  r.route_type
from gtfs.stop_times_norm n
join trips t on n.trip_id = t.trip_id
join routes r on t.route_id = r.route_id;

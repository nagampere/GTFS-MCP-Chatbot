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

-- Stop areas: uses parent_station if present, else falls back to stop_id.
-- This helps build station-level dashboards without losing platform-level detail.
create or replace view gtfs.stop_areas as
select
  s.stop_id,
  coalesce(s.parent_station, s.stop_id) as stop_area_id,
  s.stop_name,
  s.stop_lat,
  s.stop_lon,
  s.location_type,
  s.parent_station
from stops s;

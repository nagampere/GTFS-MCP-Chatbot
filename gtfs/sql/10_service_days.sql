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

-- Expands calendar into (service_id, service_date), then applies calendar_dates exceptions.
create or replace view gtfs.service_days as
with cal as (
  select
    service_id,
    start_date,
    end_date,
    monday, tuesday, wednesday, thursday, friday, saturday, sunday
  from calendar
),
date_span as (
  -- Generate all dates covered by each calendar row
  select
    c.service_id,
    d::date as service_date,
    c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday
  from cal c
  cross join generate_series(c.start_date, c.end_date, interval 1 day) as t(d)
),
weekday_filtered as (
  select
    service_id,
    service_date
  from date_span
  where
    case extract('dow' from service_date)
      when 0 then sunday      -- 0=Sunday in DuckDB
      when 1 then monday
      when 2 then tuesday
      when 3 then wednesday
      when 4 then thursday
      when 5 then friday
      when 6 then saturday
    end = 1
),
exceptions as (
  -- exception_type: 1=added, 2=removed
  select
    service_id,
    date as service_date,
    exception_type
  from calendar_dates
),
applied as (
  select
    w.service_id,
    w.service_date
  from weekday_filtered w
  left join exceptions e
    on w.service_id = e.service_id and w.service_date = e.service_date
  where coalesce(e.exception_type, 0) <> 2

  union all

  select
    e.service_id,
    e.service_date
  from exceptions e
  where e.exception_type = 1
)
select distinct service_id, service_date
from applied;

-- 20260919_nx_worker_lanes.sql
-- ADDITIVE ONLY. Not applied. Creates the owner-editable lane table that PC workers,
-- Codex windows, and GitHub Actions read to know which states they may claim.
-- Collision safety still comes from public.agenarys_durable_next_targets (FOR UPDATE SKIP LOCKED);
-- lanes only spread geography and make progress visible per window.
-- Rollback: drop view business_intelligence.nx_lane_status; drop table business_intelligence.nx_worker_lanes;

create table if not exists business_intelligence.nx_worker_lanes (
  lane_code        text primary key,                       -- W1..W9
  lane_name        text not null,
  lane_kind        text not null default 'collect'         -- collect | enrich
                   check (lane_kind in ('collect','enrich')),
  assigned_pc      text,                                   -- PC-A | PC-B | PC-C (informational)
  assigned_window  integer,                                -- 1..3 (informational)
  states           text[] not null default '{}',           -- claim scope; empty for enrich lanes
  priority_order   text[] not null default '{}',           -- states first-to-last inside the lane
  batch_limit      integer not null default 25 check (batch_limit between 1 and 30),
  max_hours        integer not null default 8  check (max_hours between 1 and 24),
  enabled          boolean not null default true,
  notes            text,
  updated_by       text,
  updated_at       timestamptz not null default now()
);

alter table business_intelligence.nx_worker_lanes enable row level security;

-- Six collection lanes balanced by open collection_targets cells (2026-09-19 counts in notes).
insert into business_intelligence.nx_worker_lanes
  (lane_code, lane_name, lane_kind, assigned_pc, assigned_window, states, priority_order, notes, updated_by)
values
  ('W1', 'Northeast', 'collect', 'PC-A', 1,
   '{NY,NJ,PA,CT,MA,RI,VT,NH,ME,MD,DE,DC,VA,WV}',
   '{PA,VA,NJ,MD,MA,NH,ME,RI,VT,DE,WV,DC,NY,CT}',
   '59,763 open cells; NY and CT already deep so they are last', 'blueprint-2026-09-19'),
  ('W2', 'Southeast', 'collect', 'PC-A', 2,
   '{FL,GA,NC,SC,AL,MS,TN}',
   '{GA,NC,TN,SC,AL,MS,FL}',
   '78,920 open cells; FL already deep so it is last', 'blueprint-2026-09-19'),
  ('W3', 'South Central', 'collect', 'PC-B', 3,
   '{TX,OK,LA,AR,NM}',
   '{TX,OK,LA,NM,AR}',
   '71,921 open cells; AR medium-covered so it is last', 'blueprint-2026-09-19'),
  ('W4', 'Midwest East', 'collect', 'PC-B', 4,
   '{OH,MI,IN,IL,WI,MN,KY}',
   '{IL,IN,OH,MI,WI,MN,KY}',
   '76,636 open cells; nearly all near-empty today — pilot lane', 'blueprint-2026-09-19'),
  ('W5', 'Midwest West', 'collect', 'PC-C', 5,
   '{MO,KS,IA,NE,SD,ND}',
   '{MO,KS,IA,NE,SD,ND}',
   '63,189 open cells', 'blueprint-2026-09-19'),
  ('W6', 'West', 'collect', 'PC-C', 6,
   '{CA,AZ,NV,UT,ID,OR,WA,AK,HI,CO,MT,WY}',
   '{WA,OR,UT,ID,NV,CO,MT,WY,AK,HI,AZ,CA}',
   '54,523 open cells; CA and AZ already covered so they are last', 'blueprint-2026-09-19'),
  ('W7', 'Enrich A', 'enrich', 'PC-A', 3, '{}', '{}',
   'Email/hours discovery from business websites; claims businesses with website and no email', 'blueprint-2026-09-19'),
  ('W8', 'Enrich B', 'enrich', 'PC-B', 3, '{}', '{}',
   'Email/hours discovery from business websites', 'blueprint-2026-09-19'),
  ('W9', 'Enrich C', 'enrich', 'PC-C', 3, '{}', '{}',
   'Email/hours discovery from business websites', 'blueprint-2026-09-19')
on conflict (lane_code) do nothing;

-- Six-line status a Codex window may print (cheap; no log reading).
create or replace view business_intelligence.nx_lane_status as
select
  l.lane_code,
  l.enabled,
  cardinality(l.states)                                         as state_count,
  (select count(*) from business_intelligence.collection_targets ct
     where ct.state = any(l.states) and ct.cycle_status = 'ready')          as ready_cells,
  (select count(*) from business_intelligence.collection_targets ct
     where ct.state = any(l.states) and ct.cycle_status = 'collecting')     as collecting_now,
  (select count(*) from business_intelligence.collection_targets ct
     where ct.state = any(l.states) and ct.cycle_status = 'completed')      as completed_cells,
  (select coalesce(sum(h.rows_collected),0) from business_intelligence.nexa_collector_heartbeats h
     where h.component_name = 'pc-worker'
       and h.execution_id like l.lane_code || '-%'
       and h.created_at > now() - interval '24 hours')                       as rows_last_24h,
  (select max(h.created_at) from business_intelligence.nexa_collector_heartbeats h
     where h.component_name = 'pc-worker'
       and h.execution_id like l.lane_code || '-%')                          as last_heartbeat
from business_intelligence.nx_worker_lanes l
order by l.lane_code;

-- Service-role read for workers (public wrapper, SECURITY DEFINER, mirrors nexa_* wrappers).
create or replace function public.nx_read_lane(p_lane_code text)
returns table (lane_code text, lane_kind text, states text[], priority_order text[], batch_limit integer, max_hours integer, enabled boolean)
language sql security definer set search_path = public, business_intelligence as $$
  select lane_code, lane_kind, states, priority_order, batch_limit, max_hours, enabled
  from business_intelligence.nx_worker_lanes where lane_code = upper(p_lane_code);
$$;

create or replace function public.nx_lane_status(p_lane_code text default null)
returns setof business_intelligence.nx_lane_status
language sql security definer set search_path = public, business_intelligence as $$
  select * from business_intelligence.nx_lane_status
  where p_lane_code is null or lane_code = upper(p_lane_code);
$$;

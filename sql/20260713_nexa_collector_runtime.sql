begin;

create table if not exists business_intelligence.nexa_collector_control (
  control_key text primary key default 'primary',
  collection_enabled boolean not null default false,
  pilot_mode boolean not null default true,
  batch_size integer not null default 1 check (batch_size between 1 and 10),
  max_concurrency integer not null default 1 check (max_concurrency between 1 and 8),
  allowed_states text[] not null default array['FL']::text[],
  paused_source_families text[] not null default '{}'::text[],
  updated_by text not null default 'owner',
  updated_at timestamptz not null default now(),
  constraint nexa_collector_control_singleton check (control_key = 'primary')
);

create table if not exists business_intelligence.nexa_collector_heartbeats (
  id uuid primary key default gen_random_uuid(),
  component_name text not null check (component_name in ('collector', 'guardian')),
  execution_id text not null,
  status text not null check (
    status in ('started', 'progress', 'completed', 'error', 'sleeping', 'quarantined')
  ),
  target_id uuid references business_intelligence.collection_targets(id),
  state text,
  county text,
  category_slug text,
  source_family text,
  rows_collected integer not null default 0 check (rows_collected >= 0),
  error_fingerprint text,
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create table if not exists business_intelligence.nexa_target_leases (
  target_id uuid primary key references business_intelligence.collection_targets(id) on delete cascade,
  lease_owner text not null,
  execution_id text not null,
  lease_expires_at timestamptz not null,
  last_progress_at timestamptz not null default now(),
  attempt_count integer not null default 1 check (attempt_count between 1 and 3),
  error_fingerprint text,
  updated_at timestamptz not null default now()
);

create table if not exists business_intelligence.nexa_collector_incidents (
  id uuid primary key default gen_random_uuid(),
  incident_key text not null unique,
  incident_type text not null,
  target_id uuid references business_intelligence.collection_targets(id),
  collector_execution_id text,
  guardian_execution_id text,
  repair_action text,
  repair_attempts integer not null default 0 check (repair_attempts between 0 and 2),
  incident_status text not null default 'open' check (
    incident_status in ('open', 'repaired', 'quarantined', 'owner_action')
  ),
  evidence jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists nexa_collector_heartbeats_component_created_idx
  on business_intelligence.nexa_collector_heartbeats (component_name, created_at desc);
create index if not exists nexa_collector_heartbeats_execution_idx
  on business_intelligence.nexa_collector_heartbeats (execution_id, created_at desc);
create index if not exists nexa_target_leases_expiration_idx
  on business_intelligence.nexa_target_leases (lease_expires_at, last_progress_at);
create index if not exists nexa_collector_incidents_status_updated_idx
  on business_intelligence.nexa_collector_incidents (incident_status, updated_at desc);

alter table business_intelligence.nexa_collector_control enable row level security;
alter table business_intelligence.nexa_collector_heartbeats enable row level security;
alter table business_intelligence.nexa_target_leases enable row level security;
alter table business_intelligence.nexa_collector_incidents enable row level security;

revoke all on table business_intelligence.nexa_collector_control from public, anon, authenticated;
revoke all on table business_intelligence.nexa_collector_heartbeats from public, anon, authenticated;
revoke all on table business_intelligence.nexa_target_leases from public, anon, authenticated;
revoke all on table business_intelligence.nexa_collector_incidents from public, anon, authenticated;

grant select, insert, update, delete on table business_intelligence.nexa_collector_control to service_role;
grant select, insert, update, delete on table business_intelligence.nexa_collector_heartbeats to service_role;
grant select, insert, update, delete on table business_intelligence.nexa_target_leases to service_role;
grant select, insert, update, delete on table business_intelligence.nexa_collector_incidents to service_role;

insert into business_intelligence.nexa_collector_control (
  control_key,
  collection_enabled,
  pilot_mode,
  batch_size,
  max_concurrency,
  allowed_states,
  paused_source_families,
  updated_by
)
values ('primary', false, true, 1, 1, array['FL']::text[], '{}'::text[], 'design-approved-default')
on conflict (control_key) do nothing;

create or replace function business_intelligence.nexa_claim_targets(
  p_worker_name text,
  p_execution_id text,
  p_states text[],
  p_limit integer default 1,
  p_lease_minutes integer default 15
)
returns table (
  id uuid,
  state text,
  county text,
  category_slug text,
  collected_count integer,
  target_goal integer,
  priority integer,
  cycle_status text,
  source_family text,
  lease_expires_at timestamptz
)
language plpgsql
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
declare
  v_enabled boolean := false;
  v_allowed_states text[] := '{}'::text[];
  v_paused_families text[] := '{}'::text[];
  v_max_concurrency integer := 1;
  v_active_leases integer := 0;
  v_limit integer := greatest(least(p_limit, 10), 1);
  v_lease_minutes integer := greatest(least(p_lease_minutes, 30), 5);
begin
  if nullif(trim(p_worker_name), '') is null or nullif(trim(p_execution_id), '') is null then
    raise exception 'worker name and execution id are required';
  end if;

  perform pg_advisory_xact_lock(hashtext('business_intelligence.nexa_claim_targets'));

  select collection_enabled, allowed_states, paused_source_families, max_concurrency
    into v_enabled, v_allowed_states, v_paused_families, v_max_concurrency
  from business_intelligence.nexa_collector_control
  where control_key = 'primary';

  if not coalesce(v_enabled, false) then
    return;
  end if;

  select count(*)::integer
    into v_active_leases
  from business_intelligence.nexa_target_leases lease
  where lease.lease_expires_at > now();

  v_limit := least(v_limit, greatest(v_max_concurrency - v_active_leases, 0));
  if v_limit <= 0 then
    return;
  end if;

  return query
  with candidates as (
    select
      ct.id,
      ct.state,
      ct.county,
      ct.category_slug,
      ct.collected_count,
      ct.target_goal,
      ct.priority,
      case
        when ct.category_slug in (
          'doctor', 'dentist', 'medical-services', 'urgent-care', 'physical-therapy',
          'chiropractors', 'pharmacies', 'optometrists', 'veterinarians',
          'medical-clinics', 'mental-health-clinics'
        ) then 'npi_registry'
        else 'osm_fallback'
      end as source_family
    from business_intelligence.collection_targets ct
    where ct.cycle_status = 'ready'
      and ct.collected_count < ct.target_goal
      and ct.state = any(v_allowed_states)
      and (p_states is null or cardinality(p_states) = 0 or ct.state = any(p_states))
      and not exists (
        select 1
        from business_intelligence.nexa_target_leases active_lease
        where active_lease.target_id = ct.id
          and active_lease.lease_expires_at > now()
      )
      and not (
        case
          when ct.category_slug in (
            'doctor', 'dentist', 'medical-services', 'urgent-care', 'physical-therapy',
            'chiropractors', 'pharmacies', 'optometrists', 'veterinarians',
            'medical-clinics', 'mental-health-clinics'
          ) then 'npi_registry'
          else 'osm_fallback'
        end = any(v_paused_families)
      )
    order by ct.priority asc, ct.updated_at asc, ct.state, ct.county, ct.category_slug
    for update of ct skip locked
    limit v_limit
  ), claimed as (
    update business_intelligence.collection_targets ct
       set cycle_status = 'collecting',
           updated_at = now(),
           notes = coalesce(ct.notes, '') || E'\nNEXA leased by ' || p_worker_name || ' at ' || now()::text
      from candidates c
     where ct.id = c.id
    returning
      ct.id,
      ct.state,
      ct.county,
      ct.category_slug,
      ct.collected_count,
      ct.target_goal,
      ct.priority,
      c.source_family
  ), leased as (
    insert into business_intelligence.nexa_target_leases (
      target_id,
      lease_owner,
      execution_id,
      lease_expires_at,
      last_progress_at,
      attempt_count,
      updated_at
    )
    select
      c.id,
      p_worker_name,
      p_execution_id,
      now() + make_interval(mins => v_lease_minutes),
      now(),
      1,
      now()
    from claimed c
    on conflict (target_id) do update
      set lease_owner = excluded.lease_owner,
          execution_id = excluded.execution_id,
          lease_expires_at = excluded.lease_expires_at,
          last_progress_at = excluded.last_progress_at,
          attempt_count = least(business_intelligence.nexa_target_leases.attempt_count + 1, 3),
          error_fingerprint = null,
          updated_at = now()
      where business_intelligence.nexa_target_leases.lease_expires_at <= now()
         or business_intelligence.nexa_target_leases.execution_id = excluded.execution_id
    returning target_id, business_intelligence.nexa_target_leases.lease_expires_at
  )
  select
    c.id,
    c.state,
    c.county,
    c.category_slug,
    c.collected_count,
    c.target_goal,
    c.priority,
    'collecting'::text,
    c.source_family,
    l.lease_expires_at
  from claimed c
  join leased l on l.target_id = c.id
  order by c.state, c.county, c.priority, c.category_slug;
end;
$$;

create or replace function business_intelligence.nexa_write_heartbeat(
  p_component_name text,
  p_execution_id text,
  p_status text,
  p_target_id uuid default null,
  p_details jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
declare
  v_id uuid;
  v_target business_intelligence.collection_targets%rowtype;
  v_rows integer := greatest(coalesce((p_details->>'rows_collected')::integer, 0), 0);
begin
  if nullif(trim(p_execution_id), '') is null then
    raise exception 'execution id is required';
  end if;

  if p_target_id is not null then
    select * into v_target
    from business_intelligence.collection_targets
    where business_intelligence.collection_targets.id = p_target_id;
  end if;

  insert into business_intelligence.nexa_collector_heartbeats (
    component_name,
    execution_id,
    status,
    target_id,
    state,
    county,
    category_slug,
    source_family,
    rows_collected,
    error_fingerprint,
    details
  ) values (
    p_component_name,
    p_execution_id,
    p_status,
    p_target_id,
    v_target.state,
    v_target.county,
    v_target.category_slug,
    nullif(p_details->>'source_family', ''),
    v_rows,
    nullif(p_details->>'error_fingerprint', ''),
    coalesce(p_details, '{}'::jsonb)
  )
  returning id into v_id;

  if p_target_id is not null and p_status in ('progress', 'completed') then
    update business_intelligence.nexa_target_leases
       set last_progress_at = now(),
           updated_at = now()
     where target_id = p_target_id
       and execution_id = p_execution_id;
  end if;

  if p_target_id is not null and p_status = 'completed' then
    delete from business_intelligence.nexa_target_leases
     where target_id = p_target_id
       and execution_id = p_execution_id;
  end if;

  return v_id;
end;
$$;

create or replace function business_intelligence.nexa_requeue_target(
  p_target_id uuid,
  p_execution_id text,
  p_reason text
)
returns boolean
language plpgsql
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
declare
  v_released uuid;
  v_failures integer := 0;
  v_blocked boolean := false;
begin
  if nullif(trim(p_execution_id), '') is null then
    raise exception 'execution id is required';
  end if;

  delete from business_intelligence.nexa_target_leases lease
   where lease.target_id = p_target_id
     and lease.execution_id = p_execution_id
  returning lease.target_id into v_released;

  if v_released is null then
    return false;
  end if;

  select count(*)::integer
    into v_failures
  from business_intelligence.nexa_collector_heartbeats heartbeat
  where heartbeat.component_name = 'collector'
    and heartbeat.target_id = p_target_id
    and heartbeat.status = 'error'
    and heartbeat.error_fingerprint = p_reason;

  v_blocked := v_failures >= 2;

  update business_intelligence.collection_targets
     set cycle_status = case when v_blocked then 'blocked' else 'ready' end,
         updated_at = now(),
         notes = coalesce(notes, '') || E'\nNEXA ' || case when v_blocked then 'blocked' else 'requeued' end || ' by ' || p_execution_id || ': ' || left(coalesce(p_reason, 'unspecified'), 200)
   where id = p_target_id
     and cycle_status = 'collecting';

  return v_blocked;
end;
$$;

create or replace function business_intelligence.nexa_release_stale_lease(
  p_target_id uuid,
  p_guardian_execution_id text,
  p_expected_execution_id text
)
returns boolean
language plpgsql
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
declare
  v_released uuid;
begin
  if nullif(trim(p_guardian_execution_id), '') is null then
    raise exception 'guardian execution id is required';
  end if;

  delete from business_intelligence.nexa_target_leases lease
   where lease.target_id = p_target_id
     and lease.execution_id = p_expected_execution_id
     and lease.lease_expires_at <= now()
     and lease.last_progress_at <= now() - interval '15 minutes'
  returning lease.target_id into v_released;

  if v_released is null then
    return false;
  end if;

  update business_intelligence.collection_targets
     set cycle_status = 'ready',
         updated_at = now(),
         notes = coalesce(notes, '') || E'\nNEXA guardian released stale lease at ' || now()::text
   where id = p_target_id
     and cycle_status = 'collecting';

  perform business_intelligence.nexa_write_heartbeat(
    'guardian',
    p_guardian_execution_id,
    'progress',
    p_target_id,
    jsonb_build_object('repair_action', 'RELEASE_STALE_LEASE')
  );

  return true;
end;
$$;

create or replace function business_intelligence.nexa_record_incident(
  p_incident_key text,
  p_incident_type text,
  p_target_id uuid,
  p_collector_execution_id text,
  p_guardian_execution_id text,
  p_repair_action text,
  p_evidence jsonb default '{}'::jsonb
)
returns table (
  incident_id uuid,
  repair_attempts integer,
  incident_status text
)
language plpgsql
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
begin
  if nullif(trim(p_incident_key), '') is null then
    raise exception 'incident key is required';
  end if;

  return query
  insert into business_intelligence.nexa_collector_incidents (
    incident_key,
    incident_type,
    target_id,
    collector_execution_id,
    guardian_execution_id,
    repair_action,
    repair_attempts,
    incident_status,
    evidence
  ) values (
    p_incident_key,
    p_incident_type,
    p_target_id,
    p_collector_execution_id,
    p_guardian_execution_id,
    p_repair_action,
    1,
    'open',
    coalesce(p_evidence, '{}'::jsonb)
  )
  on conflict (incident_key) do update
    set guardian_execution_id = excluded.guardian_execution_id,
        repair_action = excluded.repair_action,
        repair_attempts = least(business_intelligence.nexa_collector_incidents.repair_attempts + 1, 2),
        incident_status = case
          when business_intelligence.nexa_collector_incidents.repair_attempts + 1 >= 2
            then 'quarantined'
          else business_intelligence.nexa_collector_incidents.incident_status
        end,
        evidence = business_intelligence.nexa_collector_incidents.evidence || excluded.evidence,
        updated_at = now()
  returning
    business_intelligence.nexa_collector_incidents.id,
    business_intelligence.nexa_collector_incidents.repair_attempts,
    business_intelligence.nexa_collector_incidents.incident_status;
end;
$$;

create or replace function business_intelligence.nexa_guardian_snapshot()
returns jsonb
language plpgsql
stable
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
declare
  v_control business_intelligence.nexa_collector_control%rowtype;
  v_latest_heartbeat business_intelligence.nexa_collector_heartbeats%rowtype;
  v_suspect_lease business_intelligence.nexa_target_leases%rowtype;
  v_ready_count integer := 0;
  v_repeated_errors integer := 0;
  v_repair_attempts integer := 0;
begin
  select * into v_control
  from business_intelligence.nexa_collector_control
  where control_key = 'primary';

  select count(*)::integer into v_ready_count
  from business_intelligence.collection_targets ct
  where ct.cycle_status = 'ready'
    and ct.collected_count < ct.target_goal
    and ct.state = any(coalesce(v_control.allowed_states, '{}'::text[]));

  select * into v_latest_heartbeat
  from business_intelligence.nexa_collector_heartbeats heartbeat
  where heartbeat.component_name = 'collector'
    and coalesce((heartbeat.details->>'test_only')::boolean, false) = false
  order by heartbeat.created_at desc
  limit 1;

  select * into v_suspect_lease
  from business_intelligence.nexa_target_leases lease
  order by
    (lease.lease_expires_at <= now()) desc,
    lease.last_progress_at asc,
    lease.updated_at asc
  limit 1;

  if v_latest_heartbeat.error_fingerprint is not null then
    select count(*)::integer into v_repeated_errors
    from business_intelligence.nexa_collector_heartbeats heartbeat
    where heartbeat.component_name = 'collector'
      and heartbeat.error_fingerprint = v_latest_heartbeat.error_fingerprint
      and heartbeat.created_at >= now() - interval '30 minutes'
      and coalesce((heartbeat.details->>'test_only')::boolean, false) = false;
  end if;

  if v_suspect_lease.target_id is not null then
    select coalesce(max(incident.repair_attempts), 0)::integer
      into v_repair_attempts
    from business_intelligence.nexa_collector_incidents incident
    where incident.target_id = v_suspect_lease.target_id
      and incident.incident_status in ('open', 'quarantined');
  end if;

  return jsonb_build_object(
    'collection_enabled', coalesce(v_control.collection_enabled, false),
    'pilot_mode', coalesce(v_control.pilot_mode, true),
    'batch_size', coalesce(v_control.batch_size, 1),
    'allowed_states', coalesce(v_control.allowed_states, '{}'::text[]),
    'ready_count', v_ready_count,
    'last_collector_heartbeat', v_latest_heartbeat.created_at,
    'last_progress_at', coalesce(v_suspect_lease.last_progress_at, v_latest_heartbeat.created_at),
    'target_id', v_suspect_lease.target_id,
    'collector_execution_id', v_suspect_lease.execution_id,
    'lease_expired', coalesce(v_suspect_lease.lease_expires_at <= now(), false),
    'repeated_target_count', coalesce(v_suspect_lease.attempt_count, 0),
    'repeated_error_count', v_repeated_errors,
    'repair_attempts', v_repair_attempts,
    'error_fingerprint', v_latest_heartbeat.error_fingerprint,
    'checked_at', now()
  );
end;
$$;

grant usage on schema business_intelligence to service_role;

create or replace function public.nexa_read_collector_control()
returns jsonb
language sql
stable
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select to_jsonb(control_row)
  from business_intelligence.nexa_collector_control control_row
  where control_row.control_key = 'primary';
$$;

create or replace function public.nexa_claim_targets(
  p_worker_name text,
  p_execution_id text,
  p_states text[],
  p_limit integer default 1,
  p_lease_minutes integer default 15
)
returns table (
  id uuid,
  state text,
  county text,
  category_slug text,
  collected_count integer,
  target_goal integer,
  priority integer,
  cycle_status text,
  source_family text,
  lease_expires_at timestamptz
)
language sql
volatile
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select *
  from business_intelligence.nexa_claim_targets(
    p_worker_name,
    p_execution_id,
    p_states,
    p_limit,
    p_lease_minutes
  );
$$;

create or replace function public.nexa_write_heartbeat(
  p_component_name text,
  p_execution_id text,
  p_status text,
  p_target_id uuid default null,
  p_details jsonb default '{}'::jsonb
)
returns uuid
language sql
volatile
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select business_intelligence.nexa_write_heartbeat(
    p_component_name,
    p_execution_id,
    p_status,
    p_target_id,
    p_details
  );
$$;

create or replace function public.nexa_requeue_target(
  p_target_id uuid,
  p_execution_id text,
  p_reason text
)
returns boolean
language sql
volatile
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select business_intelligence.nexa_requeue_target(
    p_target_id,
    p_execution_id,
    p_reason
  );
$$;

create or replace function public.nexa_release_stale_lease(
  p_target_id uuid,
  p_guardian_execution_id text,
  p_expected_execution_id text
)
returns boolean
language sql
volatile
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select business_intelligence.nexa_release_stale_lease(
    p_target_id,
    p_guardian_execution_id,
    p_expected_execution_id
  );
$$;

create or replace function public.nexa_record_incident(
  p_incident_key text,
  p_incident_type text,
  p_target_id uuid,
  p_collector_execution_id text,
  p_guardian_execution_id text,
  p_repair_action text,
  p_evidence jsonb default '{}'::jsonb
)
returns table (
  incident_id uuid,
  repair_attempts integer,
  incident_status text
)
language sql
volatile
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select *
  from business_intelligence.nexa_record_incident(
    p_incident_key,
    p_incident_type,
    p_target_id,
    p_collector_execution_id,
    p_guardian_execution_id,
    p_repair_action,
    p_evidence
  );
$$;

create or replace function public.nexa_guardian_snapshot()
returns jsonb
language sql
stable
security invoker
set search_path = business_intelligence, public, pg_temp
as $$
  select business_intelligence.nexa_guardian_snapshot();
$$;

revoke all on function business_intelligence.nexa_claim_targets(text, text, text[], integer, integer) from public, anon, authenticated;
revoke all on function business_intelligence.nexa_write_heartbeat(text, text, text, uuid, jsonb) from public, anon, authenticated;
revoke all on function business_intelligence.nexa_requeue_target(uuid, text, text) from public, anon, authenticated;
revoke all on function business_intelligence.nexa_release_stale_lease(uuid, text, text) from public, anon, authenticated;
revoke all on function business_intelligence.nexa_record_incident(text, text, uuid, text, text, text, jsonb) from public, anon, authenticated;
revoke all on function business_intelligence.nexa_guardian_snapshot() from public, anon, authenticated;

grant execute on function business_intelligence.nexa_claim_targets(text, text, text[], integer, integer) to service_role;
grant execute on function business_intelligence.nexa_write_heartbeat(text, text, text, uuid, jsonb) to service_role;
grant execute on function business_intelligence.nexa_requeue_target(uuid, text, text) to service_role;
grant execute on function business_intelligence.nexa_release_stale_lease(uuid, text, text) to service_role;
grant execute on function business_intelligence.nexa_record_incident(text, text, uuid, text, text, text, jsonb) to service_role;
grant execute on function business_intelligence.nexa_guardian_snapshot() to service_role;

revoke all on function public.nexa_read_collector_control() from public, anon, authenticated;
revoke all on function public.nexa_claim_targets(text, text, text[], integer, integer) from public, anon, authenticated;
revoke all on function public.nexa_write_heartbeat(text, text, text, uuid, jsonb) from public, anon, authenticated;
revoke all on function public.nexa_requeue_target(uuid, text, text) from public, anon, authenticated;
revoke all on function public.nexa_release_stale_lease(uuid, text, text) from public, anon, authenticated;
revoke all on function public.nexa_record_incident(text, text, uuid, text, text, text, jsonb) from public, anon, authenticated;
revoke all on function public.nexa_guardian_snapshot() from public, anon, authenticated;

grant execute on function public.nexa_read_collector_control() to service_role;
grant execute on function public.nexa_claim_targets(text, text, text[], integer, integer) to service_role;
grant execute on function public.nexa_write_heartbeat(text, text, text, uuid, jsonb) to service_role;
grant execute on function public.nexa_requeue_target(uuid, text, text) to service_role;
grant execute on function public.nexa_release_stale_lease(uuid, text, text) to service_role;
grant execute on function public.nexa_record_incident(text, text, uuid, text, text, text, jsonb) to service_role;
grant execute on function public.nexa_guardian_snapshot() to service_role;

commit;

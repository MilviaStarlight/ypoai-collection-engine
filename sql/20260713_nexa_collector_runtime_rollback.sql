begin;

drop function if exists public.nexa_guardian_snapshot();
drop function if exists public.nexa_record_incident(text, text, uuid, text, text, text, jsonb);
drop function if exists public.nexa_release_stale_lease(uuid, text, text);
drop function if exists public.nexa_requeue_target(uuid, text, text);
drop function if exists public.nexa_write_heartbeat(text, text, text, uuid, jsonb);
drop function if exists public.nexa_claim_targets(text, text, text[], integer, integer);
drop function if exists public.nexa_read_collector_control();

drop function if exists business_intelligence.nexa_guardian_snapshot();
drop function if exists business_intelligence.nexa_record_incident(text, text, uuid, text, text, text, jsonb);
drop function if exists business_intelligence.nexa_release_stale_lease(uuid, text, text);
drop function if exists business_intelligence.nexa_requeue_target(uuid, text, text);
drop function if exists business_intelligence.nexa_write_heartbeat(text, text, text, uuid, jsonb);
drop function if exists business_intelligence.nexa_claim_targets(text, text, text[], integer, integer);

drop table if exists business_intelligence.nexa_collector_incidents;
drop table if exists business_intelligence.nexa_target_leases;
drop table if exists business_intelligence.nexa_collector_heartbeats;
drop table if exists business_intelligence.nexa_collector_control;

commit;

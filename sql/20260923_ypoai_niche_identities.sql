-- 20260923_ypoai_niche_identities.sql
-- STATUS: APPLIED 2026-09-24 after the owner confirmed Resend verification of yellowpagesofai.com and a
-- successful reply test (hello@yellowpagesofai.com -> <owner inbox> via Bluehost forwarder + Purelymail).
-- Owner decision 2026-09-23: niche sender identities move from agenarys.com to yellowpagesofai.com.
-- Additive: clones the 469 rows of public.outreach_inboxes onto the new domain and leaves the agenarys.com rows
-- untouched but inactive for the YPOAI flow. Requires Resend domain verification of yellowpagesofai.com first
-- (DKIM + return-path records published, DMARC added) and an inbound route for replies (Resend Inbound or a
-- Purelymail catch-all on the domain). No mailboxes need to exist anywhere: with Resend + a catch-all, the
-- 469 addresses are just From / Reply-To strings and reply routing keys.
--
-- Rollback: delete from public.outreach_inboxes where inbox_email like '%@yellowpagesofai.com';
--           update public.outreach_inboxes set is_active = true where inbox_email like '%@agenarys.com';

begin;

insert into public.outreach_inboxes (inbox_email, vertical_group, daily_cap, emails_sent_today, last_reset_date, is_active, language)
select split_part(inbox_email, '@', 1) || '@yellowpagesofai.com',
       vertical_group,
       50,              -- per-identity fair-share limit; the real ceiling is the domain-level warm-up cap in ypoai_send_queue
       0,
       current_date,
       true,
       language
from public.outreach_inboxes
where inbox_email like '%@agenarys.com'
  and not exists (
    select 1 from public.outreach_inboxes x
    where x.inbox_email = split_part(outreach_inboxes.inbox_email, '@', 1) || '@yellowpagesofai.com');

-- Keep the agenarys.com identities but park them; the YPOAI flow reads only active rows on the YPOAI domain.
update public.outreach_inboxes set is_active = false where inbox_email like '%@agenarys.com';

-- Verify before commit: expect 469 new rows, 0 active agenarys rows.
select
  count(*) filter (where inbox_email like '%@yellowpagesofai.com') as ypoai_rows,
  count(*) filter (where inbox_email like '%@agenarys.com' and is_active) as agenarys_active,
  count(distinct vertical_group) as vertical_groups
from public.outreach_inboxes;

commit;

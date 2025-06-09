create or replace table fail_fix_suggestions (
    fail_fix_suggestions_key bigint generated always as identity,
    fail_rsn_key bigint not null,
    fix_suggestion string not null
)
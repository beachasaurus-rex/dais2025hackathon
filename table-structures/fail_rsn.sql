create or replace table fail_rsn (
    rsn_key bigint generated always as identity,
    rsn_when timestamp not null,
    rsn string not null,
    notebook_path string not null
)
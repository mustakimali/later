-- Add migration script here
create table later_storage (
    key text primary key,
    value BYTEA not null,
    update_count int not null,
    date_created TIMESTAMPTZ not null,
    date_updated TIMESTAMPTZ null,
    date_expire TIMESTAMPTZ null
);

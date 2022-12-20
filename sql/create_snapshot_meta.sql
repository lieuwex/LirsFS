CREATE TABLE IF NOT EXISTS snapshot_meta (
    -- There is only ever one entry in this table
    id                integer primary key CHECK (id = 1),

    term              integer not null,

    last_applied_log  integer not null,

    membership        blob not null
);
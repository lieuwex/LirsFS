CREATE TABLE IF NOT EXISTS last_applied_entries (
    -- Per node, keep track of the last entry it applied to its state machine
    -- so that we can avoid applying the same entry twice.
    node_id                 integer primary key,
    log_index               integer not null,
    request_id              blob not null,
    contents                blob not null
);
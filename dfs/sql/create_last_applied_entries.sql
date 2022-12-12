CREATE TABLE IF NOT EXISTS last_applied_entries (
    -- Per node, keep track of the last entry it applied to its state machine
    -- so that we can avoid applying the same entry twice.
    node_name               text primary key,
    last_entry_id           integer not null,
    last_entry_contents     blob not null
);
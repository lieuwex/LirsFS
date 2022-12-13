CREATE TABLE IF NOT EXISTS files (
    path               text primary key,
    is_file            boolean not null check (is_file or (size = 0 and hash is null and replication_factor = 0)),
    size               integer not null,
    -- hash is in big-endian byte order.
    -- if hash = NULL: no keeper has the last write committed.
    hash               blob,
    replication_factor integer not null
);

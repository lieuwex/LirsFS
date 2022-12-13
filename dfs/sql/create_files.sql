CREATE TABLE IF NOT EXISTS files (
    id                 integer primary key,
    path               text not null,
    is_file            boolean not null check (is_file or (size = 0 and hash is null and replication_factor = 0)),
    size               integer not null,
    modified_at        integer not null, -- unix second timestamp
    -- hash is in big-endian byte order.
    -- if hash = NULL: no keeper has the last write committed.
    hash               blob,
    replication_factor integer not null
);
CREATE UNIQUE INDEX IF NOT EXISTS path_idx ON files (path);

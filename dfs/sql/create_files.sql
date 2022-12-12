CREATE TABLE IF NOT EXISTS files (
    id                 integer primary key,
    path               text not null,
    size               integer not null,
    -- hash is in big-endian byte order.
    -- if hash = NULL: no keeper has the last write committed.
    hash               blob,
    replication_factor integer not null
);
CREATE UNIQUE INDEX IF NOT EXISTS path_idx ON files (path);

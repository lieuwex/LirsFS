CREATE TABLE IF NOT EXISTS files (
    id                 integer primary key,
    path               text not null,
    size               integer not null,
    -- hash is in little-endian byte order
    hash               blob not null,
    replication_factor integer not null
);
CREATE UNIQUE INDEX IF NOT EXISTS path_idx ON files (path);

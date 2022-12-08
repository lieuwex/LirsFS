CREATE TABLE IF NOT EXISTS files (
    id                 integer primary key,
    path               text not null,
    size               integer not null,
    hash               blob not null,
    replication_factor integer not null
);
CREATE INDEX IF NOT EXISTS path_idx ON files (path);

CREATE TABLE IF NOT EXISTS files (
    id                 integer primary key,
    path               text not null,
    hash               blob not null,
    replication_factor integer not null
);
CREATE INDEX path_idx ON files (path);
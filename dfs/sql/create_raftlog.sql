CREATE TABLE IF NOT EXISTS raftlog (
    id          integer primary key,
    term        integer not null,
    entry       blob not null
);
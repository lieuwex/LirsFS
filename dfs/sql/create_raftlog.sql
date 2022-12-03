CREATE TABLE IF NOT EXISTS raftlog (
    id          integer primary key,
    command     text not null
);
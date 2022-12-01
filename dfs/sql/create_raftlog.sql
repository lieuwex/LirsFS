CREATE TABLE IF NOT EXISTS raftlog (
    id          integer primary key,
    path        text,
    sha512      text,
    node_id     integer,
);
CREATE TABLE IF NOT EXISTS keepers (
    id          integer primary key,
    path        text not null,
    node_id     integer not null,
    FOREIGN KEY(node_id) REFERENCES nodes(id)
    FOREIGN KEY(path) REFERENCES files(path)
);
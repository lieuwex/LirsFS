CREATE TABLE IF NOT EXISTS keepers (
    id          integer primary key,
    path        text not null,
    node_id     integer not null,

    -- latest calculated hash for this file for this keeper.
    -- hash is in big-endian byte order.
    hash                blob,

    FOREIGN KEY(node_id) REFERENCES nodes(id)
    FOREIGN KEY(path) REFERENCES files(path)
);

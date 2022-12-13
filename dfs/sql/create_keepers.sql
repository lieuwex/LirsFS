CREATE TABLE IF NOT EXISTS keepers (
    id          integer primary key,
    file_id     integer not null,
    node_id     integer not null,

    -- latest calculated hash for this file for this keeper.
    -- hash is in big-endian byte order.
    hash                blob not null,

    FOREIGN KEY(node_id) REFERENCES node(id)
    FOREIGN KEY(file_id) REFERENCES file(id)
);

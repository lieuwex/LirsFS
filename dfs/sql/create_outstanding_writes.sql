CREATE TABLE IF NOT EXISTS outstanding_writes (
    -- REVIEW: I think serial numbers are globally unique.
    serial      integer primary key not null,
    file_id     integer not null,
    node_id     integer not null,

    FOREIGN KEY(node_id) REFERENCES node(id)
    FOREIGN KEY(file_id) REFERENCES file(id)
);

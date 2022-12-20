CREATE TABLE IF NOT EXISTS outstanding_writes (
    -- REVIEW: I think serial numbers are globally unique.
    request_id  blob primary key not null,
    file_path   text not null,
    node_id     integer not null,

    FOREIGN KEY(node_id) REFERENCES nodes(id),
    FOREIGN KEY(file_path) REFERENCES files(path) ON DELETE CASCADE
);

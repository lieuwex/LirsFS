CREATE TABLE IF NOT EXISTS nodes (
    id          integer primary key,
    active      integer not null default 1 check (active in (0,1))
);
CREATE TABLE IF NOT EXISTS nodes (
    id          integer primary key,

    -- Status is 0: Active, 1: Inactive, 2: Lost
    status      integer not null default 1 check (status in (0,1,2))
);
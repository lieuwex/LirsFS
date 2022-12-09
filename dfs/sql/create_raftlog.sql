CREATE TABLE IF NOT EXISTS raftlog (
    id          integer primary key,
    term        integer not null,
    entry       blob not null,
    -- 0: Blank, 1: Normal, 2: ConfigChange, 3: SnapshotPointer
    entry_type  integer not null CHECK( entry_type in (0, 1, 2, 3) )
);
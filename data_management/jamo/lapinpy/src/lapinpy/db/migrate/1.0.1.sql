create table modules(
    name varchar(32),
    path varchar(256),
    CONSTRAINT name_uniq_idx UNIQUE (name)
)

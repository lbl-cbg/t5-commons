create table setting(
    id integer PRIMARY KEY,
    application varchar(32) not null,
    setting varchar(32) not null,
    value varchar(1024) not null
);

BEGIN TRANSACTION;
create table application_tokens(
    id INTEGER  not null primary key AUTOINCREMENT,
    application_id INTEGER  not null,
    token varchar(32) not null,
    created timestamp default (datetime('now')),
    CONSTRAINT user_token_idx UNIQUE (token),
    foreign key (application_id) references `application` (id),
    CONSTRAINT token_idx UNIQUE (token)
);

insert into application_tokens  select null, id, token, datetime('now') from application;

CREATE TEMPORARY TABLE t1_backup(a,b,d);
INSERT INTO t1_backup SELECT id, name, `group` FROM application;
DROP TABLE application;

create table application(
    id INTEGER  not null primary key AUTOINCREMENT,
    name varchar(100) not null,
    `group` varchar(32) not null
);

INSERT INTO application SELECT a,b,d FROM t1_backup;
DROP TABLE t1_backup;

COMMIT;

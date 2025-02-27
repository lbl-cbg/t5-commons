drop table if exists permission_group;
drop table if exists user_permissions;
drop table if exists user_tokens;
drop table if exists user;
drop table if exists menu;
drop table if exists chart_conf;
drop table if exists application_permissions;
drop table if exists permission;
drop table if exists application;
drop table if exists modules;
drop table if exists setting;

create table modules(
    name varchar(32),
    path varchar(256),
    CONSTRAINT name_uniq_idx UNIQUE (name)
);

create table application(
    id INTEGER  not null primary key AUTOINCREMENT,
    name varchar(100) not null,
    `group` varchar(32) not null,
    division varchar(64) not null default 'jgi'
);

create table permission(
    id INTEGER  not null primary key AUTOINCREMENT,
    name varchar(32) not null,
    CONSTRAINT name_idx UNIQUE (name)
);


create table application_permissions(
    id INTEGER  not null primary key AUTOINCREMENT,
    application INTEGER  not null,
    permission INTEGER  not null,
    foreign key (permission) references permission (id),
    foreign key (application) references application (id)
) ;


create table chart_conf(
    id INTEGER  not null primary key AUTOINCREMENT,
    url varchar(126) not null,
    conf longtext not null,
    CONSTRAINT url_idx UNIQUE (url)
);


create table menu(
    id INTEGER  not null primary key AUTOINCREMENT,
    parent INTEGER  not null,
    place tinyint not null,
    href varchar(126) not null,
    title varchar(126) not null
);
create table user(
    user_id INTEGER  not null primary key AUTOINCREMENT,
    email varchar(126) not null,
    name varchar(126) not null,
    `group` varchar(32),
    division varchar(64) not null default 'jgi',
    CONSTRAINT user_email_idx UNIQUE (email)
);

create table user_tokens(
    id INTEGER  not null primary key AUTOINCREMENT,
    user_id INTEGER  not null,
    token varchar(32) not null,
    CONSTRAINT user_token_idx UNIQUE (token)
    foreign key (user_id) references `user` (user_id)
);

create table application_tokens(
    id INTEGER  not null primary key AUTOINCREMENT,
    application_id INTEGER  not null,
    token varchar(32) not null,
    created timestamp default (datetime('now')), 
    CONSTRAINT user_token_idx UNIQUE (token),
    foreign key (application_id) references `application` (id),
    CONSTRAINT token_idx UNIQUE (token)
);

create table user_permissions(
    id INTEGER  not null primary key AUTOINCREMENT,
    user_id INTEGER  not null,
    permission INTEGER  not null,
    foreign key (user_id) references user (user_id),
    foreign key (permission) references permission (id)
);


create table permission_group(
    id INTEGER  not null primary key AUTOINCREMENT,
    permission INTEGER  not null,
    has_permission INTEGER  not null,
    foreign key(permission) references permission(id),
    foreign key(has_permission) references permission(id)
);

create table setting(
    id integer PRIMARY KEY,
    application varchar(32) not null, 
    setting varchar(32) not null,
    value varchar(1024) not null
);

CREATE TABLE job( 
    job_id INTEGER PRIMARY KEY AUTOINCREMENT, 
    job_path text, 
    job_name text, 
    sge_id int, 
    submitted_date timestamp default (datetime('now')), 
    started_date timestamp,
    ended_date timestamp, 
    status text, 
    exit_code int, 
    machine text, 
    command text, 
    meta_string text
);
insert into setting values (null,'core','db_version','1.0.4');

create table pmos(
  sow_item_id integer,
  pru_ids varchar(100),
  http_status_code integer not null,
  response varchar(255),
  called_date timestamp DEFAULT CURRENT_TIMESTAMP,
  primary key(sow_item_id, pru_ids)
);

-- index for pmos status values
create index pmos_http_status_code on pmos(http_status_code);
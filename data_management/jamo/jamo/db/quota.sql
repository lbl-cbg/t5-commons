drop table if exists quota;

create table quota (
    quota_id int unsigned primary key auto_increment,
    quota bigint unsigned not null,
    used bigint unsigned not null,
    percent float not null,
    dt_modified timestamp not null default current_timestamp on update current_timestamp
) ENGINE = InnoDB DEFAULT CHARSET=latin1;

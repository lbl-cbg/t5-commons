drop table if exists backup_record_status_history;
drop table if exists backup_record;
drop table if exists backup_service;
drop table if exists file_status_history;
drop table if exists hook;
drop table if exists tar_record;
drop table if exists pull_queue_status_history;
drop table if exists pull_queue;
drop table if exists file;
drop table if exists backup_record_status_cv;
drop table if exists file_status_cv;
drop table if exists transaction;
drop table if exists service;
drop table if exists md5_queue;
drop table if exists transfer_queue;
drop table if exists task_queue;
drop table if exists queue_status_cv;
drop table if exists request;
drop table if exists egress;

create table queue_status_cv(
    queue_status_id tinyint unsigned not null primary key,
    status varchar(64) not null,
    description varchar(255) default null
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table task_queue(
    task_id int unsigned primary key auto_increment,
    task_status_id tinyint unsigned not null default 1,
    task_name varchar(32) not null,
    task_features varchar(1024) not null,
    data longtext not null,
    dt_modified timestamp not null default current_timestamp on update current_timestamp,
    KEY task_status_id_fk (task_status_id),
    CONSTRAINT task_status_id_fk FOREIGN KEY (task_status_id) REFERENCES queue_status_cv(queue_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table md5_queue(
    md5_queue_id int unsigned primary key auto_increment,
    file_path varchar(512) not null,
    queue_status_id tinyint unsigned not null default 1,
    file_size bigint unsigned not null,
    md5sum varchar(64) default null,
    dt_modified timestamp not null default current_timestamp on update current_timestamp,
    callback varchar(512) not null,
    division varchar(64) not null default 'jgi',
    KEY md5queue_status_id_fk (queue_status_id),
    CONSTRAINT md5queue_status_id_fk FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv(queue_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table service(
    service_id int unsigned primary key auto_increment,
    submited_dt timestamp null,
    started_dt timestamp null,
    ended_dt timestamp null,
    seconds_to_run int unsigned default 0,
    last_heartbeat timestamp null,
    available_threads tinyint default 1,
    hostname varchar(126) null
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table file_status_cv(
    file_status_id tinyint unsigned not null primary key,
    status varchar(64) not null,
    description varchar(255) default null
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table backup_record_status_cv(
    backup_record_status_id tinyint unsigned not null primary key,
    status varchar(64) not null,
    description varchar(255) default null
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table transaction(
    transaction_id int unsigned not null primary key auto_increment,
    started timestamp not null default current_timestamp,
    finished datetime null
)ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table file(
    file_id int unsigned not null primary key auto_increment,
    transaction_id int unsigned default 1,
    file_name varchar(256) not null,
    file_path varchar(256) not null,
    origin_file_name varchar(256) null,
    origin_file_path varchar(256) null,
    file_size bigint unsigned not null,
    file_date datetime not null,
    file_owner varchar(128) default null,
    file_group varchar(128) default null,
    file_permissions varchar(12) null,
    division varchar(64) not null default 'jgi',
    local_purge_days int null,
    remote_purge_days int null,
    md5sum varchar(64) default null,
    file_status_id tinyint unsigned default 1,
    created_dt datetime default '0000-00-00 00:00:00',
    modified_dt timestamp not null default current_timestamp on update current_timestamp,
    validate_mode tinyint default 0,
    user_save_till datetime null,
    metadata_id varchar(32) default null,
    auto_uncompress boolean default false,
    source varchar(100) default null,
    key file_status_id_fk (file_status_id),
    key transaction_id_fk (transaction_id),
    UNIQUE key unique_file_key (file_path, file_name, file_size, file_date),
    CONSTRAINT file_status_id_cv_fk FOREIGN KEY (file_status_id) REFERENCES file_status_cv (file_status_id),
    CONSTRAINT transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES transaction (transaction_id)
)ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table pull_queue(
    pull_queue_id int unsigned primary key auto_increment,
    file_id int unsigned not null,
    queue_status_id tinyint unsigned not null default 1,
    dt_modified timestamp not null default current_timestamp on update current_timestamp,
    callback varchar(512),
    requestor varchar(64),
    KEY pull_queue_status_id_fk (queue_status_id),
    KEY pull_queue_file_id_fk (file_id),
    KEY pull_queue_dt_modified_queue_status_id (dt_modified, queue_status_id),
    CONSTRAINT pull_queue_status_id_fk FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv (queue_status_id),
    CONSTRAINT pull_queue_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table file_status_history(
    file_status_history_id int unsigned primary key auto_increment,
    file_id int unsigned not null,
    file_status_id tinyint unsigned not null,
    dt_begin timestamp not null default current_timestamp,
    dt_end timestamp null default null,
    key file_status_history_fk_1 (file_id),
    key file_status_history_fk_2 (file_status_id),
    CONSTRAINT file_status_history_fk_1 FOREIGN KEY (file_id) REFERENCES file (file_id),
    CONSTRAINT file_status_history_fk_2 FOREIGN KEY (file_status_id) REFERENCES file_status_cv (file_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table hook(
    hook_id int unsigned not null primary key auto_increment,
    file_id int unsigned not null,
    on_status tinyint not null,
    callback varchar(512) not null,
    created timestamp not null default CURRENT_TIMESTAMP,
    key hook_file_id_fk (file_id),
    CONSTRAINT hook_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table backup_service(
    backup_service_id tinyint not null primary key auto_increment,
    name varchar(32) not null,
    server varchar(128) not null,
    default_path varchar(256) not null,
    division varchar(64) not null default 'jgi',
    type varchar(32) not null default 'HPSS'
)ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table tar_record(
    tar_record_id int unsigned not null primary key auto_increment,
    root_path varchar(256) not null
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table backup_record(
    backup_record_id int unsigned not null primary key auto_increment,
    file_id int unsigned not null,
    service tinyint not null,
    remote_file_name varchar(256) null,
    remote_file_path varchar(256)  null,
    tar_record_id int unsigned null,
    backup_record_status_id tinyint unsigned default 1,
    md5sum varchar(64) default null,
    dt_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    dt_to_release timestamp default null,
    key file_id_backup_record_fk (file_id),
    key tar_record_id_fk (tar_record_id),
    key backup_record_status_id_cv_fk (backup_record_status_id),
    CONSTRAINT file_id_backup_record_fk FOREIGN KEY (file_id) REFERENCES file (file_id),
    CONSTRAINT backup_record_status_id_cv_fk FOREIGN KEY (backup_record_status_id) REFERENCES backup_record_status_cv(backup_record_status_id),
    CONSTRAINT tar_record_id_fk FOREIGN KEY (tar_record_id) REFERENCES tar_record (tar_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

create table backup_record_status_history(
    backup_record_status_history_id int unsigned primary key auto_increment,
    backup_record_id int unsigned not null,
    backup_record_status_id tinyint unsigned not null,
    dt_begin timestamp not null default current_timestamp,
    dt_end timestamp null default null,
    key backup_record_status_history_fk_1 (backup_record_id),
    key backup_record_status_history_fk_2 (backup_record_status_id),
    CONSTRAINT backup_record_status_history_fk_1 FOREIGN KEY (backup_record_id) REFERENCES backup_record (backup_record_id),
    CONSTRAINT backup_record_status_history_fk_2 FOREIGN KEY (backup_record_status_id) REFERENCES backup_record_status_cv (backup_record_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table pull_queue_status_history(
    pull_queue_status_history_id int unsigned primary key auto_increment,
    pull_queue_id int unsigned not null,
    queue_status_id tinyint unsigned not null,
    dt_begin timestamp not null default current_timestamp,
    dt_end timestamp null default null,
    key pull_queue_status_history_fk_1 (pull_queue_id),
    key pull_queue_status_history_fk_2 (queue_status_id),
    CONSTRAINT pull_queue_status_history_fk_1 FOREIGN KEY (pull_queue_id) REFERENCES pull_queue (pull_queue_id),
    CONSTRAINT pull_queue_status_history_fk_2 FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv (queue_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table request (
    request_id int unsigned primary key auto_increment,
    file_id int unsigned not null,
    dt_modified timestamp not null default current_timestamp on update current_timestamp,
    requestor varchar(64),
    KEY request_file_id_fk (file_id),
    CONSTRAINT request_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

-- TODO: Revisit whether we'll want to keep this table once we've properly added multiple data center egress support
create table egress(
    egress_id int unsigned not null auto_increment,
    file_id int unsigned not null,
    egress_status_id tinyint not null,
    dt_modified timestamp not null default current_timestamp on update current_timestamp,
    requestor varchar(64) default null,
    source varchar(100) default null,
    bytes_transferred bigint unsigned default null,
    request_id int unsigned default null,
    primary key(egress_id),
    INDEX egress_file_id_source_status_idx(file_id, source, egress_status_id)
) ENGINE = InnoDB DEFAULT CHARSET=latin1;

-- TODO: Revisit whether we'll want to keep this table once we've properly added multiple data center egress support
create table egress_status_history(
    egress_status_history_id int unsigned primary key auto_increment,
    egress_id int unsigned not null,
    egress_status_id tinyint unsigned not null,
    dt_begin timestamp not null default current_timestamp,
    dt_end timestamp null default null,
    key egress_status_history_fk_1(egress_id),
    key egress_status_history_fk_2(egress_status_id),
    CONSTRAINT egress_status_history_fk_1 FOREIGN KEY(egress_id) REFERENCES egress(egress_id),
    CONSTRAINT egress_status_history_fk_2 FOREIGN KEY(egress_status_id) REFERENCES queue_status_cv(queue_status_id)
)ENGINE = InnoDB DEFAULT CHARSET=latin1;

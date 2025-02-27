from argparse import ArgumentParser
import sys

from lapinpy.config_util import ConfigManager
import pymysql
from pymongo import MongoClient

# Define connection parameters
# SQL statements
drop_statements = [
    "DROP TABLE IF EXISTS backup_record_status_history;",
    "DROP TABLE IF EXISTS backup_record;",
    "DROP TABLE IF EXISTS backup_service;",
    "DROP TABLE IF EXISTS file_status_history;",
    "DROP TABLE IF EXISTS hook;",
    "DROP TABLE IF EXISTS tar_record;",
    "DROP TABLE IF EXISTS pull_queue_status_history;",
    "DROP TABLE IF EXISTS pull_queue;",
    "DROP TABLE IF EXISTS service;",
    "DROP TABLE IF EXISTS md5_queue;",
    "DROP TABLE IF EXISTS transfer_queue;",
    "DROP TABLE IF EXISTS task_queue;",
    "DROP TABLE IF EXISTS request;",
    "DROP TABLE IF EXISTS egress_status_history ;",
    "DROP TABLE IF EXISTS egress;",
    "DROP TABLE IF EXISTS file;",
    "DROP TABLE IF EXISTS transaction;",
    "DROP TABLE IF EXISTS queue_status_cv;",
    "DROP TABLE IF EXISTS backup_record_status_cv;",
    "DROP TABLE IF EXISTS quota;",

    # Drop existing triggers and views
    "DROP TRIGGER IF EXISTS file_ingest_update_trigger;",
    "DROP TRIGGER IF EXISTS file_ingest_insert_trigger;",
    "DROP TRIGGER IF EXISTS egress_insert_trigger",
    "DROP TRIGGER IF EXISTS egress_update_trigger",
    "DROP TRIGGER IF EXISTS pull_queue_insert_trigger",
    "DROP TRIGGER IF EXISTS pull_queue_update_trigger",
    "DROP TRIGGER IF EXISTS backup_record_insert_trigger",
    "DROP TRIGGER IF EXISTS backup_record_update_trigger",
    "DROP TRIGGER IF EXISTS file_insert_trigger",
    "DROP TRIGGER IF EXISTS file_update_trigger",

    "DROP VIEW IF EXISTS active;",
    "DROP VIEW IF EXISTS status;",
    "DROP TABLE IF EXISTS file_ingest_status_history;",
    "DROP TABLE IF EXISTS file_ingest;",

    "DROP TABLE IF EXISTS file_status_cv;",
]

create_statements = [
    """CREATE TABLE IF NOT EXISTS queue_status_cv (
        queue_status_id tinyint unsigned not null primary key,
        status varchar(64) not null,
        description varchar(255) default null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS task_queue (
        task_id int unsigned primary key auto_increment,
        task_status_id tinyint unsigned not null default 1,
        task_name varchar(32) not null,
        task_features varchar(1024) not null,
        data longtext not null,
        dt_modified timestamp not null default current_timestamp on update current_timestamp,
        KEY task_status_id_fk (task_status_id),
        CONSTRAINT task_status_id_fk FOREIGN KEY (task_status_id) REFERENCES queue_status_cv(queue_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS md5_queue (
        md5_queue_id int unsigned primary key auto_increment,
        file_path varchar(512) not null,
        queue_status_id tinyint unsigned not null default 1,
        file_size bigint unsigned not null,
        md5sum varchar(64) default null,
        dt_modified timestamp not null default current_timestamp on update current_timestamp,
        callback varchar(512) not null,
        division varchar(64) not null default '{default_division}',
        KEY md5queue_status_id_fk (queue_status_id),
        CONSTRAINT md5queue_status_id_fk FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv(queue_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS service (
        service_id int unsigned primary key auto_increment,
        submited_dt timestamp null,
        started_dt timestamp null,
        ended_dt timestamp null,
        seconds_to_run int unsigned default 0,
        last_heartbeat timestamp null,
        available_threads tinyint default 1,
        hostname varchar(126) null,
        tasks varchar(128) null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS file_status_cv (
        file_status_id tinyint unsigned not null primary key,
        status varchar(64) not null,
        description varchar(255) default null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS backup_record_status_cv (
        backup_record_status_id tinyint unsigned not null primary key,
        status varchar(64) not null,
        description varchar(255) default null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS transaction (
        transaction_id int unsigned not null primary key auto_increment,
        started timestamp not null default current_timestamp,
        finished datetime null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS file (
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
        division varchar(64) not null default '{default_division}',
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
        transfer_mode tinyint default 0,
        key file_status_id_fk (file_status_id),
        key transaction_id_fk (transaction_id),
        UNIQUE key unique_file_key (file_path, file_name, file_size, file_date),
        key file_name_idx (file_name),
        key origin_idx (origin_file_path, origin_file_name),
        key metadata_id_key (metadata_id),
        key file_created_dt (created_dt),
        CONSTRAINT file_status_id_cv_fk FOREIGN KEY (file_status_id) REFERENCES file_status_cv (file_status_id),
        CONSTRAINT transaction_id_fk FOREIGN KEY (transaction_id) REFERENCES transaction (transaction_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS pull_queue (
        pull_queue_id int unsigned primary key auto_increment,
        file_id int unsigned not null,
        queue_status_id tinyint unsigned not null default 1,
        dt_modified timestamp not null default current_timestamp on update current_timestamp,
        callback varchar(512),
        requestor varchar(64),
        priority tinyint default null,
        tar_record_id integer default null,
        volume char(6) default null,
        position_a bigint default null,
        position_b bigint default null,
        KEY pull_queue_status_id_fk (queue_status_id),
        KEY pull_queue_file_id_fk (file_id),
        KEY pull_queue_dt_modified (dt_modified),
        KEY tar_idx (tar_record_id),
        KEY pull_queue_status_id_dt_modified (queue_status_id, dt_modified),
        KEY pull_queue_dt_modified_queue_status_id (dt_modified, queue_status_id),
        CONSTRAINT pull_queue_status_id_fk FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv (queue_status_id),
        CONSTRAINT pull_queue_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS file_status_history (
        file_status_history_id int unsigned primary key auto_increment,
        file_id int unsigned not null,
        file_status_id tinyint unsigned not null,
        dt_begin timestamp not null default current_timestamp,
        dt_end timestamp null default null,
        key file_status_history_fk_1 (file_id),
        key file_status_history_fk_2 (file_status_id),
        CONSTRAINT file_status_history_fk_1 FOREIGN KEY (file_id) REFERENCES file (file_id),
        CONSTRAINT file_status_history_fk_2 FOREIGN KEY (file_status_id) REFERENCES file_status_cv (file_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS hook (
        hook_id int unsigned not null primary key auto_increment,
        file_id int unsigned not null,
        on_status tinyint not null,
        callback varchar(512) not null,
        created timestamp not null default CURRENT_TIMESTAMP,
        key hook_file_id_fk (file_id),
        CONSTRAINT hook_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS backup_service (
        backup_service_id tinyint not null primary key auto_increment,
        name varchar(32) not null,
        server varchar(128) not null,
        default_path varchar(256) not null,
        division varchar(64) not null default '{default_division}',
        type varchar(32) not null default 'HPSS'
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS tar_record (
        tar_record_id int unsigned not null primary key auto_increment,
        root_path varchar(256) not null
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS backup_record (
        backup_record_id int unsigned not null primary key auto_increment,
        file_id int unsigned not null,
        service tinyint not null,
        remote_file_name varchar(256) null,
        remote_file_path varchar(256) null,
        tar_record_id int unsigned null,
        backup_record_status_id tinyint unsigned default 1,
        md5sum varchar(64) default null,
        dt_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        dt_to_release timestamp default null,
        key file_id_backup_record_fk (file_id),
        key tar_record_id_fk (tar_record_id),
        key backup_record_status_id_cv_fk (backup_record_status_id),
        key file_id_backup_record_status_id (file_id, backup_record_status_id),
        key backup_record_dt_modified (dt_modified),
        key backup_record_remote_file_path (remote_file_path),
        CONSTRAINT file_id_backup_record_fk FOREIGN KEY (file_id) REFERENCES file (file_id),
        CONSTRAINT backup_record_status_id_cv_fk FOREIGN KEY (backup_record_status_id) REFERENCES backup_record_status_cv(backup_record_status_id),
        CONSTRAINT tar_record_id_fk FOREIGN KEY (tar_record_id) REFERENCES tar_record (tar_record_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS backup_record_status_history (
        backup_record_status_history_id int unsigned primary key auto_increment,
        backup_record_id int unsigned not null,
        backup_record_status_id tinyint unsigned not null,
        dt_begin timestamp not null default current_timestamp,
        dt_end timestamp null default null,
        key backup_record_status_history_fk_1 (backup_record_id),
        key backup_record_status_history_fk_2 (backup_record_status_id),
        CONSTRAINT backup_record_status_history_fk_1 FOREIGN KEY (backup_record_id) REFERENCES backup_record (backup_record_id),
        CONSTRAINT backup_record_status_history_fk_2 FOREIGN KEY (backup_record_status_id) REFERENCES backup_record_status_cv (backup_record_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS pull_queue_status_history (
        pull_queue_status_history_id int unsigned primary key auto_increment,
        pull_queue_id int unsigned not null,
        queue_status_id tinyint unsigned not null,
        dt_begin timestamp not null default current_timestamp,
        dt_end timestamp null default null,
        key pull_queue_status_history_pull_queue_id_dt_begin (pull_queue_id, dt_begin),
        key pull_queue_status_history_fk_1 (pull_queue_id),
        key pull_queue_status_history_fk_2 (queue_status_id),
        CONSTRAINT pull_queue_status_history_fk_1 FOREIGN KEY (pull_queue_id) REFERENCES pull_queue (pull_queue_id),
        CONSTRAINT pull_queue_status_history_fk_2 FOREIGN KEY (queue_status_id) REFERENCES queue_status_cv (queue_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS request (
        request_id int unsigned primary key auto_increment,
        file_id int unsigned not null,
        dt_modified timestamp not null default current_timestamp on update current_timestamp,
        requestor varchar(64),
        KEY request_file_id_fk (file_id),
        KEY request_dt_modified (dt_modified),
        CONSTRAINT request_file_id_fk FOREIGN KEY (file_id) REFERENCES file (file_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS egress (
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
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS egress_status_history (
        egress_status_history_id int unsigned primary key auto_increment,
        egress_id int unsigned not null,
        egress_status_id tinyint unsigned not null,
        dt_begin timestamp not null default current_timestamp,
        dt_end timestamp null default null,
        key egress_status_history_fk_1(egress_id),
        key egress_status_history_fk_2(egress_status_id),
        CONSTRAINT egress_status_history_fk_1 FOREIGN KEY(egress_id) REFERENCES egress(egress_id),
        CONSTRAINT egress_status_history_fk_2 FOREIGN KEY(egress_status_id) REFERENCES queue_status_cv(queue_status_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    """CREATE TABLE IF NOT EXISTS quota (
        quota_id int unsigned primary key auto_increment,
        quota bigint unsigned not null,
        used bigint unsigned not null,
        percent float not null,
        dt_modified timestamp not null default current_timestamp on update current_timestamp
       ) ENGINE = InnoDB DEFAULT CHARSET=latin1;""",

        # Create the file_ingest table
    """CREATE TABLE IF NOT EXISTS file_ingest (
        file_ingest_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        file_ingest_status_id TINYINT UNSIGNED NOT NULL DEFAULT 1,
        file_id INT NULL,
        file_size BIGINT UNSIGNED NULL DEFAULT 0,
        validate_mode TINYINT NULL,
        transfer_mode TINYINT NULL,
        local_purge_days INT,
        auto_uncompress BOOLEAN DEFAULT FALSE,
        _put_mode TINYINT DEFAULT 0,
        _is_folder BOOLEAN DEFAULT FALSE,
        _is_file BOOLEAN DEFAULT FALSE,
        _dt_modified TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        _file VARCHAR(768),
        _services VARCHAR(100) NULL,
        _destination VARCHAR(768) NULL,
        _call_source VARCHAR(10) NULL,
        _status VARCHAR(10) NULL,
        _callback VARCHAR(14) NULL,
        metadata_id VARCHAR(32) NULL,
        _metadata_ingest_id VARCHAR(32) NULL,
        file_date DATETIME NULL,
        file_owner VARCHAR(30) NULL,
        file_group VARCHAR(30) NULL,
        file_permissions VARCHAR(12) NULL,
        file_name VARCHAR(256) NULL,
        file_path VARCHAR(512) NULL,
        source VARCHAR(100) NULL,
        division VARCHAR(64) NOT NULL DEFAULT 'jgi',
        PRIMARY KEY (file_ingest_id),
        KEY file_id_fk (file_id),
        KEY file_ingest_status_id_fk (file_ingest_status_id)
       ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

        # Create the file_ingest_status_history table
    """CREATE TABLE IF NOT EXISTS file_ingest_status_history (
        file_ingest_status_history_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
        file_ingest_id INT UNSIGNED NOT NULL,
        file_ingest_status_id TINYINT UNSIGNED NOT NULL,
        dt_begin TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        dt_end TIMESTAMP NULL DEFAULT NULL,
        _status VARCHAR(10) NULL,
        KEY file_ingest_status_history_fk_1 (file_ingest_id),
        KEY file_ingest_status_history_fk_2 (file_ingest_status_id),
        CONSTRAINT file_ingest_status_history_fk_1 FOREIGN KEY (file_ingest_id) REFERENCES file_ingest (file_ingest_id),
        CONSTRAINT file_ingest_status_history_fk_2 FOREIGN KEY (file_ingest_status_id) REFERENCES file_status_cv (file_status_id)
       ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""",

    # Create views
    """CREATE VIEW active AS
        SELECT 'Ingest' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, file_ingest_status_id AS id
            FROM file_ingest
            WHERE file_ingest_status_id <> 22
            GROUP BY id
        ) AS a
        JOIN file_status_cv b ON a.id = b.file_status_id
        UNION
        SELECT 'File' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, file_status_id AS id
            FROM file
            WHERE file_status_id NOT IN (8, 10, 13)
            GROUP BY id
        ) AS a
        JOIN file_status_cv b ON a.id = b.file_status_id
        UNION
        SELECT 'Backup' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, backup_record_status_id AS id
            FROM backup_record
            WHERE backup_record_status_id <> 4
            GROUP BY id
        ) AS a
        JOIN backup_record_status_cv b ON a.id = b.backup_record_status_id
        UNION
        SELECT 'Pull' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, queue_status_id AS id
            FROM pull_queue
            WHERE queue_status_id <> 3
            GROUP BY id
        ) AS a
        JOIN queue_status_cv b ON a.id = b.queue_status_id;""",

    """CREATE VIEW status AS
        SELECT 'Ingest' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, file_ingest_status_id AS id
            FROM file_ingest
            GROUP BY id
        ) AS a
        JOIN file_status_cv b ON a.id = b.file_status_id
        UNION
        SELECT 'File' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, file_status_id AS id
            FROM file
            GROUP BY id
        ) AS a
        JOIN file_status_cv b ON a.id = b.file_status_id
        UNION
        SELECT 'Backup' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, backup_record_status_id AS id
            FROM backup_record
            GROUP BY id
        ) AS a
        JOIN backup_record_status_cv b ON a.id = b.backup_record_status_id
        UNION
        SELECT 'Pull' AS label, N, id, status
        FROM (
            SELECT COUNT(*) AS N, queue_status_id AS id
            FROM pull_queue
            GROUP BY id
        ) AS a
        JOIN queue_status_cv b ON a.id = b.queue_status_id;""",

    # Create triggers
    """CREATE TRIGGER file_ingest_update_trigger
        AFTER UPDATE ON file_ingest
        FOR EACH ROW
        BEGIN
            DECLARE _current_timestamp TIMESTAMP;
            SET _current_timestamp = NOW();
            IF OLD.file_ingest_status_id != NEW.file_ingest_status_id THEN
                UPDATE file_ingest_status_history
                SET dt_end = _current_timestamp
                WHERE file_ingest_id = NEW.file_ingest_id
                AND dt_end IS NULL;

                INSERT INTO file_ingest_status_history (
                    file_ingest_id,
                    file_ingest_status_id,
                    _status,
                    dt_begin
                ) VALUES (
                    NEW.file_ingest_id,
                    NEW.file_ingest_status_id,
                    NEW._status,
                    _current_timestamp
                );
            END IF;
        END;""",

    """CREATE TRIGGER file_ingest_insert_trigger
        AFTER INSERT ON file_ingest
        FOR EACH ROW
        BEGIN
            INSERT INTO file_ingest_status_history (
                file_ingest_id,
                file_ingest_status_id,
                _status,
                dt_begin
            ) VALUES (
                NEW.file_ingest_id,
                NEW.file_ingest_status_id,
                NEW._status,
                NOW()
            );
        END;""",

    """CREATE TRIGGER file_update_trigger
        AFTER UPDATE ON file
        FOR EACH ROW
        BEGIN
            DECLARE _current_timestamp TIMESTAMP;
            SET _current_timestamp = NOW();
            IF OLD.file_status_id != NEW.file_status_id THEN
                UPDATE file_status_history
                   SET dt_end = _current_timestamp
                 WHERE file_id = NEW.file_id
                   AND dt_end IS NULL;
                INSERT INTO file_status_history (file_id, file_status_id, dt_begin)
                VALUES (NEW.file_id, NEW.file_status_id, _current_timestamp);
            END IF;
        END;""",

    """CREATE TRIGGER file_insert_trigger
        AFTER INSERT ON file
        FOR EACH ROW
        BEGIN
            INSERT INTO file_status_history (file_id, file_status_id, dt_begin)
            VALUES (NEW.file_id, NEW.file_status_id, NOW());
        END;""",

    """CREATE TRIGGER backup_record_update_trigger
        AFTER UPDATE ON backup_record
        FOR EACH ROW
        BEGIN
            DECLARE _current_timestamp TIMESTAMP;
            SET _current_timestamp = NOW();
            IF OLD.backup_record_status_id != NEW.backup_record_status_id THEN
                UPDATE backup_record_status_history
                   SET dt_end = _current_timestamp
                 WHERE backup_record_id = NEW.backup_record_id
                   AND dt_end IS NULL;
                INSERT INTO backup_record_status_history (backup_record_id, backup_record_status_id, dt_begin)
                VALUES (NEW.backup_record_id, NEW.backup_record_status_id, _current_timestamp);
            END IF;
        END;""",

    """CREATE TRIGGER backup_record_insert_trigger
        AFTER INSERT ON backup_record
        FOR EACH ROW
        BEGIN
            INSERT INTO backup_record_status_history (backup_record_id, backup_record_status_id, dt_begin)
            VALUES (NEW.backup_record_id, NEW.backup_record_status_id, NOW());
        END;""",

        # Drop and create pull_queue triggers
    """CREATE TRIGGER pull_queue_update_trigger
        AFTER UPDATE ON pull_queue
        FOR EACH ROW
        BEGIN
            DECLARE _current_timestamp TIMESTAMP;
            SET _current_timestamp = NOW();
            IF OLD.queue_status_id != NEW.queue_status_id THEN
                UPDATE pull_queue_status_history
                   SET dt_end = _current_timestamp
                 WHERE pull_queue_id = NEW.pull_queue_id
                   AND dt_end IS NULL;
                INSERT INTO pull_queue_status_history (pull_queue_id, queue_status_id, dt_begin)
                VALUES (NEW.pull_queue_id, NEW.queue_status_id, _current_timestamp);
            END IF;
        END;""",

    """CREATE TRIGGER pull_queue_insert_trigger
        AFTER INSERT ON pull_queue
        FOR EACH ROW
        BEGIN
            INSERT INTO pull_queue_status_history (pull_queue_id, queue_status_id, dt_begin)
            VALUES (NEW.pull_queue_id, NEW.queue_status_id, NOW());
        END;""",

        # Drop and create egress triggers
    """CREATE TRIGGER egress_update_trigger
        AFTER UPDATE ON egress
        FOR EACH ROW
        BEGIN
            DECLARE _current_timestamp TIMESTAMP;
            SET _current_timestamp = NOW();
            IF OLD.egress_status_id != NEW.egress_status_id THEN
                UPDATE egress_status_history
                   SET dt_end = _current_timestamp
                 WHERE egress_id = NEW.egress_id
                   AND dt_end IS NULL;
                INSERT INTO egress_status_history (egress_id, egress_status_id, dt_begin)
                VALUES (NEW.egress_id, NEW.egress_status_id, _current_timestamp);
            END IF;
        END;""",

    """CREATE TRIGGER egress_insert_trigger
        AFTER INSERT ON egress
        FOR EACH ROW
        BEGIN
            INSERT INTO egress_status_history (egress_id, egress_status_id, dt_begin)
            VALUES (NEW.egress_id, NEW.egress_status_id, NOW());
        END;""",

]


cv_statements = [
    """INSERT INTO file_status_cv (file_status_id, status, description) VALUES
        (1, 'REGISTERED', 'File has been registered and waiting for md5'),
        (2, 'COPY_READY', 'File is ready to be copied to staging area'),
        (3, 'COPY_IN_PROGRESS', 'File is being copied to staging area'),
        (4, 'COPY_COMPLETE', 'File has been copied to staging area'),
        (5, 'COPY_FAILED', 'File failed to be copied'),
        (6, 'BACKUP_READY', 'File is ready to be pushed to tape'),
        (7, 'BACKUP_IN_PROGRESS', 'records are being backed up to tape'),
        (8, 'BACKUP_COMPLETE', 'records have been saved to tape'),
        (9, 'BACKUP_FAILED', 'backup has failed for some reason'),
        (10,'PURGED','File is no longer on disk'),
        (11,'DELETE','File is registered to be deleted'),
        (12, 'RESTORE_IN_PROGRESS', 'file is being restored'),
        (13, 'RESTORED', 'file has been restored'),
        (14, 'TAR_READY','file is ready to be tared'),
        (15, 'TAR_IN_PROGRESS', 'file is being tared'),
        (16, 'TAR_COMPLETE', 'file has been tared'),
        (17, 'TAR_FAILED', 'file failed to tar for some reason'),
        (18, 'RECORDS_TO_FIX', 'a state jamo does not use, records here are under investigation'),
        (19, 'INGEST_STATS_COMPLETE', 'File ingest stats completed'),
        (20, 'INGEST_STATS_FAILED', 'File ingest stats failed'),
        (21, 'INGEST_FILE_MISSING', 'File missing or perms issue'),
        (22, 'INGEST_COMPLETE', 'File stats applied and ingest completed'),
        (23, 'INGEST_FAILED', 'File ingest failed to complete'),
        (24, 'UNTAR_READY', 'file is ready to be untarred'),
        (25, 'UNTAR_IN_PROGRESS', 'file is being untarred'),
        (26, 'UNTAR_COMPLETE', 'file has been untarred'),
        (27, 'UNTAR_FAILED', 'file failed to untar for some reason'),
        (28, 'RESTORE_REGISTERED', 'File restore has been requested');""",

    # Insert into backup_record_status_cv
    """INSERT INTO backup_record_status_cv (backup_record_status_id, status, description) VALUES
       (1, 'REGISTERED', 'Backup record is registered and waiting for action'),
       (2, 'TRANSFER_READY', 'file is ready to be transfered'),
       (3, 'TRANSFER_IN_PROGRESS', 'file is being transfered to disk cache'),
       (4, 'TRANSFER_COMPLETE', 'file has been transfered to disk cache'),
       (5, 'TRANSFER_FAILED', 'file failed to transfer'),
       (6, 'WAIT_FOR_TAPE', 'file is waiting to be put on to tape'),
       (7, 'ON_TAPE', 'file is on tape'),
       (8, 'MD5_PREP', 'md5 job has been submitted to the cluster'),
       (9, 'MD5_IN_PROGRESS', 'md5 job is running'),
       (10, 'MD5_COMPLETE', 'md5 has completed'),
       (11, 'MD5_FAILED', 'md5 has failed for some reason'),
       (12, 'VALIDATION_COMPLETE', 'validate complete corectly'),
       (13, 'VALIDATION_FAILED', 'file failed validation'),
       (14, 'VALIDATION_READY', 'file is ready to be validated'),
       (15, 'VALIDATION_IN_PROGRESS', 'file is being validated'),
       (16, 'HOLD', 'hold state');""",

    # Insert into queue_status_cv
    """INSERT INTO queue_status_cv (queue_status_id, status, description) VALUES
       (0, 'HOLD', 'Hold Action - manual usage'),
       (1, 'REGISTERED', 'Action has been registered and is waiting to be run'),
       (2, 'IN_PROGRESS', 'Action is being run'),
       (3, 'COMPLETE', 'Action is complete'),
       (4, 'FAILED', 'Action failed to run'),
       (5, 'CALLBACK_FAILED', 'Callback failed'),
       (6, 'PREP_FAILED', 'Prep failed'),
       (7, 'PREP_IN_PROGRESS', 'Prep is in progress');""",

    # Insert into transaction
    "INSERT INTO transaction (finished) VALUES (NOW());",

]
# Insert into backup_service

def main(argv=None):
    """Initialize MySQL database needed for running tape application

    :param argv: command-line arguments following the subcommand this function
                 is assigned to[, optional | , default: value]
    :raises exception: description
    :return: None
    """
    parser = ArgumentParser(description="Initialize MySQL database needed "
                                        "for running tape application")
    parser.add_argument("lapin_config", help="The lapinpy config file")
    parser.add_argument("tape_config", help="The tape config file")
    parser.add_argument("metadata_config", help="The metadata config file")
    tape_args = parser.add_argument_group("Tape", "Arguments for initializing tape database")
    tape_args.add_argument("-c", "--clean", action='store_true', default=False,
                           help="drop all existing tables before creating more")
    args = parser.parse_args(argv)

    lconf = ConfigManager(args.lapin_config).settings
    lconf = lconf['shared']
    host = lconf['mysql_host']

    tconf = ConfigManager(args.tape_config).settings
    tconf.update(lconf)
    tdb = tconf['tape_db']


    sql = None
    error = False
    try:
        # Create tape user and database
        conn = pymysql.connect(host=host, user=lconf['mysql_user'], password=lconf['mysql_pass'], port=lconf.get('mysql_port'))
        cur = conn.cursor()
        cur.execute("CREATE DATABASE IF NOT EXISTS %s" % tdb)
        cur.execute("use %s" % tdb)

        if args.clean:
            print("Cleaning tape database '%s' before initializing" % tdb)
            for sql in drop_statements:
                sql = sql.format(**tconf)
                cur.execute(sql)

        print("Creating tables for tape database '%s'" % tdb)
        for sql in create_statements:
            sql = sql.format(**tconf)
            cur.execute(sql)

        # Set up backup services
        backup_svc_ins = "INSERT INTO backup_service (default_path, type, name, server) VALUES"
        for busvc in tconf['backup_services']:
            backup_svc_ins += " ('{default_path}', '{type}', '{name}', '{server}')".format(**busvc)
        backup_svc_ins += ";"
        _cv_statements = cv_statements + [backup_svc_ins]

        print("Populating controlled vocabulary tables")
        for sql in _cv_statements:
            sql = sql.format(**tconf)
            cur.execute(sql)

        conn.commit()
    except pymysql.MySQLError as e:
        print("Error when executing the following statement:\n%s" % sql, file=sys.stderr)
        print("Error: %d - %s" % e.args, file=sys.stderr)
        error = True

    finally:
        if conn:
            conn.close()
        if error:
            exit(1)

    # Connect to MongoDB with authentication
    lconf['mongoserver'] = '%s:%s' % (lconf['mongoserver'], lconf.get('mongo_port', '27017'))
    client = MongoClient('mongodb://{mongo_user}:{mongo_pass}@{mongoserver}'.format(**lconf))
    print("Connecting to MongoDB as {mongo_user} at {mongoserver}".format(**lconf))

    # Access the database
    db = client[lconf['meta_db']]

    # List of collections to create
    collections = [
        'data_store',
        'file',
        'file_ingest',
        'keys_dictionary',
        'metadata_refresh',
        'process_services',
        'subscriptions',
        'user'
    ]

    # Function to create a collection by inserting and then removing a dummy document
    def create_collection(collection_name):
        if args.clean:
            db[collection_name].drop()
        db[collection_name].insert_one({ '_id': 0, 'dummy': 'This is a dummy document' })
        db[collection_name].delete_one({ '_id': 0 })

    # Create each collection
    for collection in collections:
        create_collection(collection)
        print("Created collection '%s'" % collection)


if __name__ == '__main__':
    main()


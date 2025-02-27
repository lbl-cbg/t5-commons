DROP TRIGGER IF EXISTS file_ingest_update_trigger;
DROP TRIGGER IF EXISTS file_ingest_insert_trigger;
drop view if exists active;
drop view if exists status;
drop table if exists file_ingest_status_history;
drop table if exists file_ingest;

create table file_ingest (
    file_ingest_id int unsigned not null auto_increment,
    file_ingest_status_id tinyint unsigned not null default 1,
    file_id int null,
    file_size bigint unsigned null default 0,

    validate_mode tinyint null,
    transfer_mode tinyint null,
    local_purge_days int,
    auto_uncompress boolean default false,
    _put_mode tinyint default 0,

    _is_folder boolean default false,
    _is_file boolean default false,
    _dt_modified timestamp not null default current_timestamp on update current_timestamp,

    _file varchar(768),
    _services varchar(100) null,
    _destination varchar(768) null,
    _call_source varchar(10) null,
    _status varchar(10) null,
    _callback varchar(14) null,

    metadata_id varchar(32) null,
    _metadata_ingest_id varchar(32) null,

    file_date datetime null,
    file_owner varchar(30) null,
    file_group varchar(30) null,
    file_permissions varchar(12) null,
    division varchar(64) not null default 'jgi',

    file_name varchar(256) null,
    file_path varchar(512) null,

    source varchar(100) null,

    primary key (file_ingest_id),
    key file_id_fk (file_id),
    key file_ingest_status_id_fk (file_ingest_status_id)
) ENGINE = InnoDB DEFAULT CHARSET=latin1;

create table file_ingest_status_history (
    file_ingest_status_history_id int unsigned primary key auto_increment,
    file_ingest_id int unsigned not null,
    file_ingest_status_id tinyint unsigned not null,
    dt_begin timestamp not null default current_timestamp,
    dt_end timestamp null default null,
    _status varchar(10) null,
    key file_ingest_status_history_fk_1 (file_ingest_id),
    key file_ingest_status_history_fk_2 (file_ingest_status_id),
    CONSTRAINT file_ingest_status_history_fk_1 FOREIGN KEY (file_ingest_id) REFERENCES file_ingest (file_ingest_id),
    CONSTRAINT file_ingest_status_history_fk_2 FOREIGN KEY (file_ingest_status_id) REFERENCES file_status_cv (file_status_id)
) ENGINE = InnoDB DEFAULT CHARSET=latin1;



CREATE VIEW active AS
select 'Ingest' AS label, N, id, status from (select count(*) as N, file_ingest_status_id as id from file_ingest where file_ingest_status_id <> 22 group by id) as a join file_status_cv b on a.id = b.file_status_id
union select 'File' AS label, N, id, status from (select count(*) as N, file_status_id as id from file where file_status_id not in (8, 10, 13)group by id) as a join file_status_cv b on a.id = b.file_status_id
union select 'Backup' AS label, N, id, status from (select count(*) as N, backup_record_status_id as id from backup_record where backup_record_status_id <> 4 group by id) as a join backup_record_status_cv b on a.id = b.backup_record_status_id
union select 'Pull' AS label, N, id, status from (select count(*) as N, queue_status_id as id from pull_queue where queue_status_id <> 3 group by id) as a join queue_status_cv b on a.id = b.queue_status_id;

CREATE VIEW status AS
select 'Ingest' AS label, N, id, status from (select count(*) as N, file_ingest_status_id as id from file_ingest group by id) as a join file_status_cv b on a.id = b.file_status_id
union select 'File' AS label, N, id, status from (select count(*) as N, file_status_id as id from file group by id) as a join file_status_cv b on a.id = b.file_status_id
union select 'Backup' AS label, N, id, status from (select count(*) as N, backup_record_status_id as id from backup_record group by id) as a join backup_record_status_cv b on a.id = b.backup_record_status_id
union select 'Pull' AS label, N, id, status from (select count(*) as N, queue_status_id as id from pull_queue  group by id) as a join queue_status_cv b on a.id = b.queue_status_id;

delete from file_status_cv where file_status_id >= 19;
insert into file_status_cv values (19, 'INGEST_STATS_COMPLETE', 'File ingest stats completed');
insert into file_status_cv values (20, 'INGEST_STATS_FAILED',  'File ingest stats failed');
insert into file_status_cv values (21, 'INGEST_FILE_MISSING',  'File missing or perms issue');
insert into file_status_cv values (22, 'INGEST_COMPLETE',  'File stats applied and ingest completed');
insert into file_status_cv values (23, 'INGEST_FAILED', 'File ingest failed to complete');

DELIMITER $$

CREATE TRIGGER file_ingest_update_trigger
AFTER UPDATE ON file_ingest
    FOR EACH ROW
    BEGIN
        DECLARE _current_timestamp timestamp;
        SET _current_timestamp = NOW();
        IF OLD.file_ingest_status_id != NEW.file_ingest_status_id
        THEN
            UPDATE file_ingest_status_history
               SET dt_end = _current_timestamp
             WHERE file_ingest_id = NEW.file_ingest_id
               AND dt_end IS NULL;

            INSERT INTO file_ingest_status_history
               (
                  file_ingest_id,
                  file_ingest_status_id,
                  _status,
                  dt_begin
               )
               VALUES
               (
                  NEW.file_ingest_id,
                  NEW.file_ingest_status_id,
                  NEW._status,
                  _current_timestamp
               );
        END IF;
    END;
$$

CREATE TRIGGER file_ingest_insert_trigger
AFTER INSERT ON file_ingest
    FOR EACH ROW
    BEGIN
        INSERT INTO file_ingest_status_history
           (
              file_ingest_id,
              file_ingest_status_id,
              _status,
              dt_begin
           )
           VALUES
           (
              NEW.file_ingest_id,
              NEW.file_ingest_status_id,
              NEW._status,
              now()
           );
    END;
$$

DELIMITER ;

drop view if exists active;
drop view if exists status;
drop view if exists age;

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


CREATE VIEW age AS
select 'Ingest' as tab,
       status as state,
       'Ingest' as group_vol,
       fi.file_ingest_status_id as status_id,
       '-' as queue,
       '1' as section,
       count(*) as N,
       min(_dt_modified) min_date
  from file_ingest fi
  join file_status_cv cv
    on fi.file_ingest_status_id = cv.file_status_id
 where fi.file_ingest_status_id <> 22
 group by 1, 2, 3, 4, 5, 6

union

select 'File' as tab,
       status as state,
       if(f.file_status_id < 6, "Copy", "Backup") as group_vol,
       f.file_status_id as status_id,
       '-' as queue,
       '2' as section,
       count(*) as N,
       min(modified_dt) min_date
  from file f
  join file_status_cv cv
    on f.file_status_id = cv.file_status_id
 where f.file_status_id < 8
 group by 1, 2, 3, 4, 5, 6

union

select 'Backup_Record' as tab,
       status as state,
       'Backup' as group_vol,
       br.backup_record_status_id as status_id,
       service as queue,
       '3' as section,
       count(*) as N,
       min(dt_modified) min_date
  from backup_record br join backup_record_status_cv brcv
    on br.backup_record_status_id = brcv.backup_record_status_id
 where br.backup_record_status_id <> 4
 group by 1, 2, 3, 4, 5, 6

union

select 'Pulll_Queue' as tab,
       if(volume is null, "Prep", if(queue_status_id = 1, "Registered", "In Progress")) as state,
       if(queue_status_id = 1, "--", volume) as group_vol,
       queue_status_id as status_id,
       priority as queue,
       '4' as section,
       count(*) as N,
       min(dt_modified) min_date
 from pull_queue
where queue_status_id != 3
group by 1, 2, 3, 4, 5, 6

order by 6, 4, 2, 1, 8, 3, 5;

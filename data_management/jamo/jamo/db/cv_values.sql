insert into file_status_cv values 
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
 (28, 'RESTORE_REGISTERED', 'File restore has been requested');

insert into backup_record_status_cv values
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
 (15, 'VALIDATION_IN_PROGRESS', 'file is being validated');

insert into queue_status_cv values
(0, 'HOLD', 'Hold Action - manual usage'),
(1, 'REGISTERED', 'Action has been registered and is waiting to be run'),
(2, 'IN_PROGRESS', 'Action is being run'),
(3, 'COMPLETE', 'Action is complete'),
(4, 'FAILED', 'Action failed to run'),
(5, 'CALLBACK_FAILED', 'Callback failed'),
(6, 'PREP_FAILED', 'Prep failed'),
(7, 'PREP_IN_PROGRESS', 'Prep is in progress');


insert into transaction (finished) values (now());
insert into backup_service (default_path, type, name, server) values
 ('/home/projects/dm_archive/root', 'HPSS', 'archive', 'archive.nersc.gov'),
 ('/home/projects/dm_archive/root', 'HPSS', 'hpss', 'hpss.nersc.gov'),
 ('/proj/biff113', 'globus', 'ornl', ''),
 ('/archive/svc-jgi-archive/data', 'globus', 'emsl', '');

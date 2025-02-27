alter table md5_queue add division varchar(64) not null default 'jgi';
alter table file add division varchar(64) not null default 'jgi';
alter table backup_service add division varchar(64) not null default 'jgi';
alter table file_ingest add division varchar(64) not null default 'jgi';

alter table file_ingest add column _dt_modified timestamp not null default current_timestamp on update current_timestamp after _is_file;

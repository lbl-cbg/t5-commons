alter table pull_queue drop column callback, add column priority tinyint default null, add column tar_record_id integer default null, add column volume char(6), add column position_a bigint, add column position_b bigint;

create index pull_queue_dt_modified on pull_queue(dt_modified);

insert into queue_status_cv values (6, 'PREP_FAILED', 'Prep failed');

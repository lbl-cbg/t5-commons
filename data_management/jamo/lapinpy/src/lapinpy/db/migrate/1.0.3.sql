drop table if exists job;
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

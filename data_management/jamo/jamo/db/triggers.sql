DELIMITER $$

DROP TRIGGER IF EXISTS file_update_trigger$$
CREATE TRIGGER file_update_trigger
AFTER UPDATE ON file
    FOR EACH ROW
    BEGIN
        DECLARE _current_timestamp timestamp;
        SET _current_timestamp = NOW();
        IF OLD.file_status_id != NEW.file_status_id
        THEN
            UPDATE file_status_history
               SET dt_end = _current_timestamp
             WHERE file_id = NEW.file_id
               AND dt_end IS NULL;

            INSERT INTO file_status_history
               (
                  file_id,
                  file_status_id,
                  dt_begin
               )
               VALUES
               (
                  NEW.file_id,
                  NEW.file_status_id,
                  _current_timestamp
               );
        END IF;
    END;
$$

DROP TRIGGER IF EXISTS file_insert_trigger$$
CREATE TRIGGER file_insert_trigger
AFTER INSERT ON file
    FOR EACH ROW
    BEGIN
        INSERT INTO file_status_history
           (
              file_id,
              file_status_id,
              dt_begin
           )
           VALUES
           (
              NEW.file_id,
              NEW.file_status_id,
              now()
           );
    END;
$$

DELIMITER ;

DELIMITER $$

DROP TRIGGER IF EXISTS backup_record_update_trigger$$
CREATE TRIGGER backup_record_update_trigger
AFTER UPDATE ON backup_record
    FOR EACH ROW
    BEGIN
        DECLARE _current_timestamp timestamp;
        SET _current_timestamp = NOW();
        IF OLD.backup_record_status_id != NEW.backup_record_status_id
        THEN
            UPDATE backup_record_status_history
               SET dt_end = _current_timestamp
             WHERE backup_record_id = NEW.backup_record_id
               AND dt_end IS NULL;

            INSERT INTO backup_record_status_history
               (
                  backup_record_id,
                  backup_record_status_id,
                  dt_begin
               )
               VALUES
               (
                  NEW.backup_record_id,
                  NEW.backup_record_status_id,
                  _current_timestamp
               );
        END IF;
    END;
$$

DROP TRIGGER IF EXISTS backup_record_insert_trigger$$
CREATE TRIGGER backup_record_insert_trigger
AFTER INSERT ON backup_record
    FOR EACH ROW
    BEGIN
        INSERT INTO backup_record_status_history
           (
              backup_record_id,
              backup_record_status_id,
              dt_begin
           )
           VALUES
           (
              NEW.backup_record_id,
              NEW.backup_record_status_id,
              now()
           );
    END;
$$

DELIMITER ;

DELIMITER $$

DROP TRIGGER IF EXISTS pull_queue_update_trigger$$
CREATE TRIGGER pull_queue_update_trigger
AFTER UPDATE ON pull_queue
    FOR EACH ROW
    BEGIN
        DECLARE _current_timestamp timestamp;
        SET _current_timestamp = NOW();
        IF OLD.queue_status_id != NEW.queue_status_id
        THEN
            UPDATE pull_queue_status_history
               SET dt_end = _current_timestamp
             WHERE pull_queue_id = NEW.pull_queue_id
               AND dt_end IS NULL;

            INSERT INTO pull_queue_status_history
               (
                  pull_queue_id,
                  queue_status_id,
                  dt_begin
               )
               VALUES
               (
                  NEW.pull_queue_id,
                  NEW.queue_status_id,
                  _current_timestamp
               );
        END IF;
    END;
$$

DROP TRIGGER IF EXISTS pull_queue_insert_trigger$$
CREATE TRIGGER pull_queue_insert_trigger
AFTER INSERT ON pull_queue
    FOR EACH ROW
    BEGIN
        INSERT INTO pull_queue_status_history
           (
              pull_queue_id,
              queue_status_id,
              dt_begin
           )
           VALUES
           (
              NEW.pull_queue_id,
              NEW.queue_status_id,
              now()
           );
    END;
$$

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

DELIMITER $$

DROP TRIGGER IF EXISTS egress_update_trigger$$
CREATE TRIGGER egress_update_trigger
AFTER UPDATE ON egress
    FOR EACH ROW
    BEGIN
        DECLARE _current_timestamp timestamp;
        SET _current_timestamp = NOW();
        IF OLD.egress_status_id != NEW.egress_status_id
        THEN
            UPDATE egress_status_history
               SET dt_end = _current_timestamp
             WHERE egress_id = NEW.egress_id
               AND dt_end IS NULL;

            INSERT INTO egress_status_history
               (
                  egress_id,
                  egress_status_id,
                  dt_begin
               )
               VALUES
               (
                  NEW.egress_id,
                  NEW.egress_status_id,
                  _current_timestamp
               );
        END IF;
    END;
$$

DROP TRIGGER IF EXISTS egress_insert_trigger$$
CREATE TRIGGER egress_insert_trigger
AFTER INSERT ON egress
    FOR EACH ROW
    BEGIN
        INSERT INTO egress_status_history
           (
              egress_id,
              egress_status_id,
              dt_begin
           )
           VALUES
           (
              NEW.egress_id,
              NEW.egress_status_id,
              now()
           );
    END;
$$

DELIMITER ;

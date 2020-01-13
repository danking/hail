CREATE TABLE IF NOT EXISTS `batch_staging` (
  `batch_id` BIGINT NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `n_jobs` INT NOT NULL DEFAULT 0,
  `n_ready_jobs` INT NOT NULL DEFAULT 0,
  `ready_cores_mcpu` INT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE
) ENGINE = InnoDB;

DELIMITER $$

DROP PROCEDURE IF EXISTS insert_batch_staging_tokens;
CREATE PROCEDURE insert_batch_staging_tokens(
  IN in_batch_id INT
)
BEGIN
    DECLARE i int DEFAULT 0;
    WHILE i < 32 DO
        INSERT IGNORE INTO batch_staging (batch_id, token) VALUES (in_batch_id, i);
        SET i = i + 1;
    END WHILE;
END $$

DROP PROCEDURE IF EXISTS close_batch;
CREATE PROCEDURE close_batch(
  IN in_batch_id BIGINT,
  IN in_timestamp BIGINT,
  IN in_user VARCHAR(100)
)
BEGIN
  DECLARE cur_batch_closed BOOLEAN;
  DECLARE expected_n_jobs INT;
  DECLARE actual_n_jobs INT;

  START TRANSACTION;

  SELECT n_jobs, closed INTO expected_n_jobs, cur_batch_closed FROM batches
  WHERE id = in_batch_id AND NOT deleted;

  IF cur_batch_closed = 1 THEN
    COMMIT;
    SELECT 0 as rc;
  ELSEIF cur_batch_closed = 0 THEN
    SELECT SUM(n_jobs) INTO staging_n_jobs,
           SUM(n_ready_jobs) INTO staging_n_ready_jobs,
           SUM(ready_cores_mcpu) INTO staging_ready_cores_mcpu
    FROM batch_staging
    WHERE batch_id = in_batch_id;

    IF staging_n_jobs = expected_n_jobs THEN
      UPDATE batches SET closed = 1 WHERE id = in_batch_id;
      UPDATE user_resources
      SET n_ready_jobs = n_ready_jobs + staging_n_ready_jobs
          ready_cores_mcpu = ready_cores_mcpu + staging_ready_cores_mcpu
      WHERE user = in_user;
      UPDATE ready_cores
      SET ready_cores_mcpu = ready_cores_mcpu + staging_ready_cores_mcpu;
      UPDATE batches SET time_completed = in_timestamp
        WHERE id = in_batch_id AND n_completed = batches.n_jobs;
      COMMIT;
      SELECT 0 as rc;
    ELSE
      ROLLBACK;
      SELECT 2 as rc, expected_n_jobs, staging_n_jobs, 'wrong number of jobs' as message;
    END IF;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_batch_closed, 'batch closed is not 0 or 1' as message;
  END IF;
END $$

CREATE TABLE IF NOT EXISTS `instances_free_cores_mcpu` (
  `name` VARCHAR(100) NOT NULL,
  `free_cores_mcpu` INT NOT NULL,
  PRIMARY KEY (`name`),
  FOREIGN KEY (`name`) REFERENCES instances(`name`)
) ENGINE = InnoDB;

INSERT INTO `instance_free_cores_mcpu`
SELECT `name`, free_cores_mcpu
FROM instances;

ALTER TABLE instances DROP COLUMN free_cores_mcpu;

DELIMITER $$

DROP PROCEDURE IF EXISTS add_attempt $$
CREATE PROCEDURE add_attempt(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN in_cores_mcpu INT,
  OUT delta_cores_mcpu INT
)
BEGIN
  DECLARE attempt_exists BOOLEAN;
  DECLARE cur_instance_state VARCHAR(40);
  SET delta_cores_mcpu = IFNULL(delta_cores_mcpu, 0);

  SET attempt_exists = EXISTS (SELECT * FROM attempts
                               WHERE batch_id = in_batch_id AND
                                 job_id = in_job_id AND attempt_id = in_attempt_id
                               FOR UPDATE);

  IF NOT attempt_exists AND in_attempt_id IS NOT NULL THEN
    INSERT INTO attempts (batch_id, job_id, attempt_id, instance_name) VALUES (in_batch_id, in_job_id, in_attempt_id, in_instance_name);

    UPDATE instances, instances_free_cores_mcpu
    SET free_cores_mcpu = free_cores_mcpu - in_cores_mcpu
    WHERE instances.name = in_instance_name
      AND instances.name = instance_free_cores_mcpu.name
      AND (instances.state = 'pending' OR instances.state = 'active');

    SET delta_cores_mcpu = -1 * in_cores_mcpu;
    END IF;
  END IF;
END $$

DELIMITER ;

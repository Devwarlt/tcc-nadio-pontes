CREATE DATABASE IF NOT EXISTS `sisbin`
	DEFAULT CHARACTER SET latin1
	COLLATE latin1_swedish_ci;

USE `sisbin`;

CREATE TABLE IF NOT EXISTS `sin_subsystems` (
	`id` VARCHAR(2) NOT NULL,
	`name` VARCHAR(64) NOT NULL,
	PRIMARY KEY (`id`)
) Engine=InnoDB;

CREATE TABLE IF NOT EXISTS `sin_subsystems_reports` (
	`id` INT AUTO_INCREMENT,
	`subsystem_id` VARCHAR(2) NOT NULL,
	`instant_record` DATETIME NOT NULL,
	`instant_load_following` FLOAT NOT NULL,
	PRIMARY KEY (`id`)
) Engine=InnoDB;

CREATE TABLE IF NOT EXISTS `sin_load_peeking_history` (
	`id` INT AUTO_INCREMENT,
	`instant` DATETIME NOT NULL,
	`raw_incident_foresight` JSON NOT NULL CHECK(JSON_VALID(`raw_incident_foresight`)),
	PRIMARY KEY (`id`)
) Engine=InnoDB;

ALTER TABLE `sin_subsystems_reports`
	ADD CONSTRAINT `sin_subsystems_reports`
	FOREIGN KEY (`subsystem_id`)
	REFERENCES `sin_subsystems` (`id`)
	ON DELETE CASCADE;

INSERT INTO `sin_subsystems` VALUES ("N", "Norte");
INSERT INTO `sin_subsystems` VALUES ("NE", "Nordeste");
INSERT INTO `sin_subsystems` VALUES ("S", "Sul");
INSERT INTO `sin_subsystems` VALUES ("SE", "Sudeste/Centro-Oeste");
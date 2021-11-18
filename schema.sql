CREATE DATABASE IF NOT EXISTS `sisbin`
	DEFAULT CHARACTER SET latin1
	COLLATE latin1_swedish_ci;

USE `sisbin`;

CREATE TABLE IF NOT EXISTS `report` (
	`id` INT AUTO_INCREMENT,
	`date` DATETIME NOT NULL,
	`raw_data` MEDIUMBLOB NOT NULL,
	PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `prediction_history` (
	`id` INT AUTO_INCREMENT,
	`date` DATETIME NOT NULL,
	`incident_foresight` FLOAT NOT NULL,
	PRIMARY KEY (`id`)
);
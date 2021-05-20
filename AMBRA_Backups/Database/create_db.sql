
DROP TABLE IF EXISTS `backup_info`;
CREATE TABLE `backup_info` (
  /*`id` int NOT NULL AUTO_INCREMENT,*/
  `namespace_name` varchar(255) DEFAULT NULL,
  `namespace_type` varchar(9) NOT NULL CHECK (`namespace_type` IN ('Group', 'Location')),
  `last_backup` datetime DEFAULT NULL,
  /*PRIMARY KEY (`id`)*/
  UNIQUE KEY `id_namespace` (`namespace_name`, `namespace_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `crf`;
CREATE TABLE `crf` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_study` int DEFAULT NULL,
  `record_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `record_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `crf_name` varchar(255) DEFAULT NULL,
  `html_path` varchar(255) DEFAULT NULL,
  'csv_path' varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `sequence_name`;
CREATE TABLE `sequence_name` (
  `id` int NOT NULL AUTO_INCREMENT,
  `modality` varchar(45) DEFAULT NULL,
  `name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `sequence_map`;
CREATE TABLE `sequence_map` (
  `id` int NOT NULL AUTO_INCREMENT,
  `sequence_name` varchar(255) DEFAULT NULL,
  `id_sequence_name` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_map_id_sequence_name` FOREIGN KEY (`id_sequence_name`) REFERENCES `sequence_name` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `series_name`;
CREATE TABLE `series_name` (
  `id` int NOT NULL AUTO_INCREMENT,
  `modality` varchar(45) DEFAULT NULL,
  `name` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `series_map`;
CREATE TABLE `series_map` (
  `id` int NOT NULL AUTO_INCREMENT,
  `series_name` varchar(255) DEFAULT NULL,
  `id_series_name` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_map_id_series_name` FOREIGN KEY (`id_series_name`) REFERENCES `series_name` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `studies`;
CREATE TABLE `studies` (
  `id` int NOT NULL AUTO_INCREMENT,
  `attachment_count` int DEFAULT NULL,
  `series_count` int DEFAULT NULL,
  `study_uid` varchar(255) NOT NULL,
  `study_description` varchar(255) DEFAULT NULL,
  `id_series_name` int DEFAULT NULL,
  `is_downloaded` tinyint DEFAULT NULL,
  `download_date` datetime DEFAULT NULL,
  `updated` datetime DEFAULT NULL,
  `study_date` datetime DEFAULT NULL,
  `created_date` datetime DEFAULT NULL,
  `modality` varchar(45) DEFAULT NULL,
  `institution_id` int NOT NULL,
  `zip_path` varchar(255) DEFAULT NULL,
  `nifti_directory` varchar(255) DEFAULT NULL,
  `id_patient` int DEFAULT NULL,
  `phi_namespace` varchar(255) DEFAULT NULL,
  `storage_namespace` varchar(255) DEFAULT NULL,
  `record_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `record_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `notes` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `study_uid_UNIQUE` (`study_uid`),
  CONSTRAINT `fk_study_id_series_name` FOREIGN KEY (`id_series_name`) REFERENCES `series_name` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `img_series`;
CREATE TABLE `img_series` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_study` int NOT NULL,
  `scanner_model` varchar(255) DEFAULT NULL,
  `scanner_manufac` varchar(45) DEFAULT NULL,
  `magnetic_field_strength` float DEFAULT NULL,
  `device_serial_number` varchar(255) DEFAULT NULL,
  `protocol_name` varchar(255) DEFAULT NULL,
  `series_number` int DEFAULT NULL,
  `series_description` varchar(255) DEFAULT NULL,
  `id_sequence_name` int DEFAULT NULL,
  `TR` float DEFAULT NULL,
  `TE` float DEFAULT NULL,
  `recon_matrix_rows` int DEFAULT NULL,
  `recon_matrix_cols` int DEFAULT NULL,
  `slice_thickness` float DEFAULT NULL,
  `number_of_slices` int DEFAULT NULL,
  `number_of_temporal_positions` int DEFAULT NULL,
  `acquisition_number` int DEFAULT NULL,
  `processed_directory` varchar(255) DEFAULT NULL,
  `id_sequence` int DEFAULT NULL,
  `raw_nifti` varchar(255) DEFAULT NULL,
  `number_of_dicoms` int DEFAULT NULL,
  `series_uid` varchar(255) NOT NULL,
  `software_version` varchar(255) DEFAULT NULL,
  `pixel_spacing` varchar(255) DEFAULT NULL,
  `pixel_bandwidth` float DEFAULT NULL,
  `acq_matrix` varchar(255) DEFAULT NULL,
  `perc_phase_fov` float DEFAULT NULL,
  `inversion_time` float DEFAULT NULL,
  `flip_angle` float DEFAULT NULL,
  `scanner_station_name` varchar(255) DEFAULT NULL,
  `mr_acq_type` varchar(255) DEFAULT NULL,
  `sequence_name` varchar(255) DEFAULT NULL,
  `id_patient` int DEFAULT NULL,
  `record_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `record_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `notes` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `series_id_UNIQUE` (`id`),
  UNIQUE KEY `series_uid_UNIQUE` (`series_uid`),
  KEY `id_sequence_idx` (`id_sequence`),
  CONSTRAINT `fk_id_study` FOREIGN KEY (`id_study`) REFERENCES `studies` (`id`),
  CONSTRAINT `fk_id_sequence_name` FOREIGN KEY (`id_sequence_name`) REFERENCES `sequence_name` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `institutions`;
CREATE TABLE `institutions` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(45) DEFAULT NULL,
  `site_number` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `institutions_id_UNIQUE` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `patients`;
CREATE TABLE `patients` (
  `id` int NOT NULL AUTO_INCREMENT,
  `patient_name` varchar(45) NOT NULL,
  `patient_id` varchar(45) NOT NULL,
  `is_phantom` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idpatients_UNIQUE` (`id`),
  UNIQUE KEY `patient_name_UNIQUE` (`patient_name`),
  UNIQUE KEY `patient_id_UNIQUE` (`patient_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `processing`;
CREATE TABLE `processing` (
  `id` int NOT NULL AUTO_INCREMENT,
  `img_series_id` int DEFAULT NULL,
  `file_path` varchar(255) DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `record_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `record_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

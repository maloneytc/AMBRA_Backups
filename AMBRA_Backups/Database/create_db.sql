
DROP TABLE IF EXISTS `backup_info`;
CREATE TABLE `backup_info` (
  /*`id` int NOT NULL AUTO_INCREMENT,*/
  `namespace_name` varchar(255) DEFAULT NULL,
  `namespace_type` varchar(9) NOT NULL CHECK (`namespace_type` IN ('Group', 'Location')),
  `namespace_id` varchar(255) NOT NULL,
  `namespace_uuid` varchar(255) NOT NULL,
  `last_backup` datetime DEFAULT NULL,
  /*PRIMARY KEY (`id`)*/
  UNIQUE KEY `id_namespace` (`namespace_name`, `namespace_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Table structure for table `backup_info_RedCap`
--

DROP TABLE IF EXISTS `backup_info_RedCap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `backup_info_RedCap` (
  `project_name` varchar(255) NOT NULL,
  `last_backup` datetime DEFAULT NULL,
  PRIMARY KEY (`project_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `reports`;
CREATE TABLE `reports` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `report_name` VARCHAR(45) NULL,
  `last_run` DATETIME NULL,
  `report_type` VARCHAR(45) NULL,
  `webhook_url` VARCHAR(256) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `report_name_UNIQUE` (`report_name` ASC) VISIBLE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;;


DROP TABLE IF EXISTS `CRF`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_study` int DEFAULT NULL,
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `crf_name` varchar(255) DEFAULT NULL,
  `path` varchar(255) DEFAULT NULL,
  `file_type` varchar(255) DEFAULT NULL,
  `signed_by` varchar(255) DEFAULT NULL,
  `signed_date` timestamp NULL DEFAULT NULL,
  `uploaded` timestamp NULL DEFAULT NULL,
  `crf_id` varchar(45) DEFAULT NULL,
  `version` varchar(45) DEFAULT NULL,
  `phi_namespace` varchar(45) DEFAULT NULL,
  `data_added` tinyint(1) DEFAULT NULL,
  `file_name` varchar(45) DEFAULT NULL,
  `verified` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `crf_id_UNIQUE` (`crf_id`),
  UNIQUE KEY `path_UNIQUE` (`path`)
) ENGINE=InnoDB AUTO_INCREMENT=1127 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `CRF_Schema`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_Schema` (
  `id` int NOT NULL AUTO_INCREMENT,
  `version` varchar(45) DEFAULT NULL COMMENT 'Version of the CRF that this question belongs to.',
  `crf_name` varchar(45) DEFAULT NULL COMMENT 'Name of the CRF that this question belongs to.',
  `question_id` varchar(45) DEFAULT NULL COMMENT 'ID label of the question as it is displayed on the CRF.',
  `question_text` varchar(255) DEFAULT NULL COMMENT 'Text of the question as it is displayed on the CRF.',
  `question_sub_text` varchar(255) DEFAULT NULL,
  `data_labels` varchar(255) DEFAULT NULL COMMENT 'Label of the data entry field describing data entry format or data choices.',
  `re_pattern` varchar(128) DEFAULT NULL COMMENT 'Regular expression pattern used to verify the field value.',
  `data_type` varchar(45) DEFAULT NULL COMMENT 'Type of the field value expressed as a python data type.',
  `html_span_id` varchar(128) NOT NULL COMMENT 'ID field value of the html span element containing the CRF field value.',
  `csv_column_id` varchar(128) DEFAULT NULL,
  `variable_coding` varchar(255) DEFAULT NULL COMMENT 'Python expression to convert the field variable to a decoded variable. The expression should store the decoded value in a variable named ''decode''.',
  `na_value` varchar(45) DEFAULT NULL COMMENT 'Value to use if field is not applicable.',
  `dependencies` varchar(255) DEFAULT NULL COMMENT 'Required dependencies for this question to have a value.',
  `notes` varchar(512) DEFAULT NULL COMMENT 'Any notes related to this question.',
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_question` (`version`,`crf_name`,`html_span_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


DROP TABLE IF EXISTS `CRF_Data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_Data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_crf` int DEFAULT NULL,
  `id_schema` int DEFAULT NULL,
  `value` varchar(1024) DEFAULT NULL,
  `decoded_value` varchar(255) DEFAULT NULL,
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `html_class` varchar(45) DEFAULT NULL,
  `html_style` varchar(45) DEFAULT NULL,
  `html_span_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `crf_and_span_id` (`id_crf`,`html_span_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `CRF_Data_RedCap`
--

DROP TABLE IF EXISTS `CRF_Data_RedCap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_Data_RedCap` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_crf` int DEFAULT NULL,
  `value` varchar(1024) DEFAULT NULL,
  `redcap_variable` varchar(255) DEFAULT NULL,
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `crf_and_span_id` (`id_crf`,`redcap_variable`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1976 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `CRF_RedCap`
--

DROP TABLE IF EXISTS `CRF_RedCap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_RedCap` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_patient` int DEFAULT NULL,
  `crf_name` varchar(255) DEFAULT NULL,
  `instance` int DEFAULT NULL,
  `verified` tinyint(1) DEFAULT '0',
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `CRF_Schema_RedCap`
--

DROP TABLE IF EXISTS `CRF_Schema_RedCap`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_Schema_RedCap` (
  `id` int NOT NULL AUTO_INCREMENT,
  `crf_name` varchar(45) DEFAULT NULL COMMENT 'Name of the CRF that this question belongs to.',
  `data_id` varchar(45) DEFAULT NULL COMMENT 'id to use in reports',
  `question_text` varchar(255) DEFAULT NULL COMMENT 'Text of the question as it is displayed on the CRF.',
  `question_sub_text` varchar(255) DEFAULT NULL,
  `data_labels` varchar(255) DEFAULT NULL COMMENT 'Label of the data entry field describing data entry format or data choices.',
  `redcap_variable` varchar(45) DEFAULT NULL COMMENT 'variables used in redcap forms',
  `notes` varchar(512) DEFAULT NULL COMMENT 'Any notes related to this question.',
  `crf_section` varchar(45) DEFAULT NULL COMMENT 'section of redcap form question belongs to',
  `option_order` varchar(45) DEFAULT NULL COMMENT 'order of option for checkbox question',
  `option_col_num` varchar(45) DEFAULT NULL COMMENT 'column placement of option in checkbox question',
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_question` (`crf_name`,`redcap_variable`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `CRF_Sections`
--

DROP TABLE IF EXISTS `CRF_Sections`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `CRF_Sections` (
  `id` int NOT NULL AUTO_INCREMENT,
  `crf_name` varchar(45) DEFAULT NULL,
  `section_number` int DEFAULT NULL,
  `section_name` varchar(128) DEFAULT NULL,
  `section_description` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


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
  `series_description` varchar(255) DEFAULT NULL,
  `id_series_name` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `series_description_UNIQUE` (`series_description`),
  KEY `fk_map_id_series_name` (`id_series_name`),
  CONSTRAINT `fk_map_id_series_name` FOREIGN KEY (`id_series_name`) REFERENCES `series_name` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3435 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `studies`;
CREATE TABLE `studies` (
  `id` int NOT NULL AUTO_INCREMENT,
  `attachment_count` int DEFAULT NULL,
  `series_count` int DEFAULT NULL,
  `study_uid` varchar(255) NOT NULL,
  `uuid` varchar(255) NOT NULL,
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
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `notes` varchar(255) DEFAULT NULL,
  `viewer_link` varchar(255) DEFAULT NULL,
  `copied_to_process` tinyint(1) DEFAULT NULL,
  `must_approve` tinyint(1) DEFAULT NULL,
  `deleted` tinyint(1) DEFAULT '0' COMMENT 'Binary value indicating whether the study has been deleted from Ambra.',
  PRIMARY KEY (`id`),
  UNIQUE KEY `study_uid_UNIQUE` (`study_uid`),
  KEY `fk_study_id_series_name` (`id_series_name`),
  CONSTRAINT `fk_study_id_series_name` FOREIGN KEY (`id_series_name`) REFERENCES `series_name` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=455 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


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
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `notes` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `series_id_UNIQUE` (`id`),
  UNIQUE KEY `series_uid_UNIQUE` (`series_uid`),
  KEY `id_sequence_idx` (`id_sequence`),
  KEY `fk_id_study` (`id_study`),
  KEY `fk_id_sequence_name` (`id_sequence_name`),
  CONSTRAINT `fk_id_sequence_name` FOREIGN KEY (`id_sequence_name`) REFERENCES `sequence_name` (`id`),
  CONSTRAINT `fk_id_study` FOREIGN KEY (`id_study`) REFERENCES `studies` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=34034 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


DROP TABLE IF EXISTS `nifti_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `nifti_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `file_path` varchar(512) DEFAULT NULL,
  `json_path` varchar(512) DEFAULT NULL,
  `Modality` varchar(12) DEFAULT NULL,
  `Manufacturer` varchar(45) DEFAULT NULL,
  `ManufacturersModelName` varchar(45) DEFAULT NULL,
  `BodyPartExamined` varchar(45) DEFAULT NULL,
  `PatientPosition` varchar(45) DEFAULT NULL,
  `ProcedureStepDescription` varchar(255) DEFAULT NULL,
  `SoftwareVersions` varchar(45) DEFAULT NULL,
  `SeriesDescription` varchar(255) DEFAULT NULL,
  `ProtocolName` varchar(255) DEFAULT NULL,
  `ImageType` varchar(512) DEFAULT NULL,
  `RawImage` tinyint(1) DEFAULT NULL,
  `SeriesNumber` int DEFAULT NULL,
  `AcquisitionTime` varchar(24) DEFAULT NULL,
  `AcquisitionNumber` int DEFAULT NULL,
  `ConversionSoftware` varchar(45) DEFAULT NULL,
  `ConversionSoftwareVersion` varchar(45) DEFAULT NULL,
  `id_img_series` int DEFAULT NULL,
  `id_study` int DEFAULT NULL,
  `xdim` int DEFAULT NULL,
  `ydim` int DEFAULT NULL,
  `zdim` int DEFAULT NULL,
  `tdim` int DEFAULT NULL,
  `md5_hash` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_path_UNIQUE` (`file_path`),
  UNIQUE KEY `json_path_UNIQUE` (`json_path`),
  KEY `fk_id_img_series_idx` (`id_img_series`),
  KEY `fk_id_study_idx` (`id_study`),
  CONSTRAINT `fk_id_img_series` FOREIGN KEY (`id_img_series`) REFERENCES `img_series` (`id`),
  CONSTRAINT `fk_nii_data_id_study` FOREIGN KEY (`id_study`) REFERENCES `studies` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10539 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
  `is_phantom` tinyint DEFAULT NULL,
  `comments` varchar(120) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idpatients_UNIQUE` (`id`),
  UNIQUE KEY `patient_name_UNIQUE` (`patient_name`),
  UNIQUE KEY `patient_id_UNIQUE` (`patient_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `processing`;
CREATE TABLE `processing` (
  `id` int NOT NULL AUTO_INCREMENT,
  `img_series_id` int DEFAULT NULL,
  `file_path` varchar(512) DEFAULT NULL,
  `file_name` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `record_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `record_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `hash` varchar(255) DEFAULT NULL,
  `image_preview_path` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS `annotations`;
CREATE TABLE `annotations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_study` int DEFAULT NULL,
  `file_path` varchar(512) DEFAULT NULL,
  `record_created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `file_path_UNIQUE` (`file_path`)
) ENGINE=InnoDB AUTO_INCREMENT=21 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `segmentations`;
CREATE TABLE `segmentations` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_img_series` int DEFAULT NULL,
  `roi_path` varchar(1024) DEFAULT NULL,
  `description` varchar(256) DEFAULT NULL,
  `notes` varchar(255) DEFAULT NULL,
  `record_created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_UNIQUE` (`id`),
  CONSTRAINT `id_id_img_series` FOREIGN KEY (`id`) REFERENCES `img_series` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=120 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE IF EXISTS `study_tags`;
CREATE TABLE `study_tags` (
  `id` int NOT NULL AUTO_INCREMENT,
  `id_study` int DEFAULT NULL,
  `tag_group` varchar(4) DEFAULT NULL,
  `tag_element` varchar(4) DEFAULT NULL,
  `tag_value` varchar(512) DEFAULT NULL,
  `record_created` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `record_updated` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `study_tags_id_study_unq` (`id_study`,`tag_group`,`tag_element`),
  KEY `fk_study_tags_1_idx` (`id_study`)
) ENGINE=InnoDB AUTO_INCREMENT=12973 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

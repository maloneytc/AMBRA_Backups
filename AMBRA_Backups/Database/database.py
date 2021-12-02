import os
from pathlib import Path
import logging
import pdb
from datetime import datetime
from mysql.connector import connect, Error
import configparser
from string import Template
import hashlib

import pandas as pd

################################################################################
class Database():
    # --------------------------------------------------------------------------
    def __init__(self, database, config_path=None):
        """
        Initialize the Database class.

        Inputs:
        --------
        database: str
            Name of the database to connect to.
        config_path: str, Path
            Path to the config file, if 'None' it will look for the file in
            ~/.study_database
        """
        self.db_name = database
        self.config_path = config_path
        self.connection = self.connect(self.db_name, config_path=config_path)

    # --------------------------------------------------------------------------
    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close()

    # --------------------------------------------------------------------------
    def reconnect(self):
        self.connection.close()
        self.connection = self.connect(self.db_name, config_path=self.config_path)

    # --------------------------------------------------------------------------
    @classmethod
    def get_config(cls, config_path=None):
        """
        Gets user credentials from config file with the following format:
        [ambra_backup]
        user_name = XXX
        password = XXX
        host = XXX
        port = XXX

        If the config_path input is None, it will look for the file ~/.study_database
        """
        if config_path:
            config_file = Path(config_path)
        else:
            config_file = Path.home().joinpath('.study_database')
        if not config_file.exists():
            logging.error(f'Could not find the credentials file: {config_file}')

        config = configparser.ConfigParser()
        config.read(config_file)

        return config

    # --------------------------------------------------------------------------
    @classmethod
    def connect(cls, db_name=None, config_path=None):
        """
        """
        config = cls.get_config(config_path=config_path)
        db_config = config['ambra_backup']

        try:
            connection = connect(
                host = db_config['host'],
                port = db_config['port'],
                user = db_config['user_name'],
                password = db_config['password'],
                database = db_name,
                #pool_size = 500
                #buffered=True,
                #consume_results=True
            )
        except Error as e:
            print(e)
            logging.error(e)

        return connection

    # --------------------------------------------------------------------------
    @classmethod
    def get_databases(cls, config_path=None):
        """
        """
        connection = cls.connect(config_path=config_path)

        #TODO: Check that the database does not already exist

        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES;")
            databases = [this[0] for this in cursor]

        return databases

    # --------------------------------------------------------------------------
    @classmethod
    def create_db(cls, db_name, config_path=None):
        """
        """
        if db_name in cls.get_databases(config_path=config_path):
            raise Exception('Database already exists.')

        connection = cls.connect(config_path=config_path)

        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {db_name};")
        connection.commit()

    # --------------------------------------------------------------------------
    @classmethod
    def create_schema(cls, db_name, config_path=None):
        """
        """
        template_file = Path(__file__).parent.joinpath('create_db.sql')
        with open(template_file, 'r') as fopen:
            template_string = fopen.readlines()
        template_string = ''.join(template_string)
        db_template = Template(template_string)
        queries = db_template.substitute(db_name=db_name)

        try:
            cls.create_db(db_name, config_path=config_path)
        except:
            pass

        connection = cls.connect(db_name, config_path=config_path)

        with connection.cursor() as cursor:
            for query in queries.split(';'):
                cursor.execute(query.strip())
            connection.commit()

    # --------------------------------------------------------------------------
    def list_tables(self, buffered=True):
        with self.connection.cursor(buffered=buffered) as cursor:
            cursor.execute("SHOW TABLES;")
            results = cursor.fetchall()
        return list(results)

    # --------------------------------------------------------------------------
    def describe_table(self, table_name, buffered=True):
        with self.connection.cursor(buffered=buffered) as cursor:
            cursor.execute(f"DESCRIBE {table_name};")
            results = cursor.fetchall()
        return list(results)

    # --------------------------------------------------------------------------
    def run_select_query(self, query, column_names=False, buffered=True):
        """
        Runs an SQL SELECT query and return the results.

        Do not set buffered to True is you expect a large result to be returned.
        """
        # with self.connection.cursor(buffered=buffered) as cursor:
        #     cursor.execute(query)
        #     while True:
        #         results = cursor.fetchmany(10)
        #         if not results:
        #             break
        #         for result in results:
        #             yield result
        with self.connection.cursor(buffered=buffered) as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            columns = cursor.description

        self.connection.commit()
        if column_names:
            result_dicts = [{columns[index][0]:column for index, column in enumerate(value)} for value in results]
            return result_dicts

        return list(results)

    # --------------------------------------------------------------------------
    def run_insert_query(self, query, record):
        """
        Runs an SQL INSERT/UPDATE query.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, record)

        self.connection.commit()

    # --------------------------------------------------------------------------
    def insert_dict(self, dict, table):
        """
        Insert the dictionary into the specified table with keys being the column names.
        """
        query = f"INSERT INTO {table} ( " + ", ".join(dict.keys()) + ") " + \
                "VALUES ( " + ", ".join(["%s" for this in dict.values()]) + " );"

        self.run_insert_query(query, tuple(dict.values()))

    # --------------------------------------------------------------------------
    def update_dict(self, dict, table, id_column, id_value):
        """
        Update the dictionary into the specified table with keys being the column
        names for the 'id_column' row with value 'id_value'.
        """
        set_string = ', '.join([str(this)+'=%s' for this in dict.keys()])
        query = f"UPDATE {table} SET {set_string} WHERE {id_column}='{id_value}';"

        self.run_insert_query(query, tuple(dict.values()))

    # --------------------------------------------------------------------------
    def insert_update_datetime(self, namespace_name, namespace_type, namespace_id, namespace_uuid, date_time):
        """
        Inserts the datetime into the last_backup column of the info table.

        Inputs:
        -------
        namepace_name:

        namespace_type:

        date_time: datetime object
            Date and time of the last database sync with ambra.
        """
        assert namespace_type in ['Group', 'Location']

        insert_update_query = """
        INSERT INTO backup_info (namespace_name, namespace_type, namespace_id, namespace_uuid, last_backup)
        VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE last_backup=%s;
        """
        datetime_record = (namespace_name, namespace_type, namespace_id, namespace_uuid, date_time.strftime('%Y-%m-%d %H:%M:%S'),date_time.strftime('%Y-%m-%d %H:%M:%S'))
        with self.connection.cursor() as cursor:
            cursor.execute(insert_update_query, datetime_record)

        self.connection.commit()

    # --------------------------------------------------------------------------
    def get_last_backup(self, namespace_name, namespace_type):
        """
        Returns a datetime object of the last_update column of the backup_info table.
        """
        # TODO: Raise exception if a namespace with the given name and type does not exist!

        get_update_query = """
            SELECT last_backup FROM backup_info WHERE namespace_name=%s and namespace_type=%s;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(get_update_query, (namespace_name, namespace_type))
            result = cursor.fetchone()

        if result is None:
            return None
        return result[0]

    # --------------------------------------------------------------------------
    def get_study_by_uid(self, uid):
        """
        Returns the id from the studies table for the study that matches the
        given uid.

        Inputs:
        ----------
        uid: str
            uid of the study to retrieve.
        """
        select_query = """SELECT id FROM studies WHERE studies.study_uid=%s"""
        with self.connection.cursor() as cursor:
            cursor.execute(select_query, (uid, ))
            result = cursor.fetchone()

        if result is None:
            return None
        return result[0]

    # --------------------------------------------------------------------------
    def get_series_by_uid(self, uid):
        """
        Returns the id from the img_series table for the series that matches the
        given uid
        """
        select_query = """SELECT id FROM img_series WHERE img_series.series_uid=%s"""
        with self.connection.cursor() as cursor:
            cursor.execute(select_query, (uid, ))
            result = cursor.fetchone()

        if result is None:
            return None
        return result[0]

    # --------------------------------------------------------------------------
    def insert_patient(self, patient_id, patient_name):
        """
        Insert the patient into the database if it does not already exist.

        Inputs:
        -----------
        patient_id:

        patient_name:
        """
        insert_patient_query = """
        INSERT IGNORE INTO patients (patient_id, patient_name)
        VALUES (%s, %s);
        """
        patient_record = [(patient_id, patient_name)]
        with self.connection.cursor() as cursor:
            cursor.executemany(insert_patient_query, patient_record)

        self.connection.commit()

    # --------------------------------------------------------------------------
    def insert_study(self, study):
        """
        Because study_uid is set as a unique primary key and IGNORE is used in the query,
        if the study is already in the database nothing will happen.

        Inputs:
        -----------
        study: Object of the AMBRA_Utils.Study class
            Object of the study class to be added to the database.
        """

        if study.updated[-3:] == '-07':
            updated_string = study.updated[0:-3]
        else:
            updated_string = study.updated
        try:
            study_updated = datetime.strptime(updated_string, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            study_updated = datetime.strptime(updated_string, '%Y-%m-%d %H:%M:%S')

        if study.created[-3:] == '-07':
            created_string = study.created[0:-3]
        else:
            created_string = study.created
        try:
            study_created = datetime.strptime(created_string, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            study_created = datetime.strptime(created_string, '%Y-%m-%d %H:%M:%S')

        #print(f'Study date: {study.study_date}')
        if study.study_date:
            try:
                study_date = datetime.strptime(study.study_date, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    study_date = datetime.strptime(study.study_date, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        study_date = datetime.strptime(study.study_date, '%Y-%m-%d')
                    except ValueError:
                        try:
                            study_date = datetime.strptime(study.study_date, '%Y%m%d')
                        except ValueError:
                            try:
                                study_date = datetime.strptime(study.study_date, '%m/%d/%Y')
                            except:
                                try:
                                    study_date = datetime.strptime(study.study_date, '%d/%m/%Y')
                                except:
                                    study_date = None
        else:
            study_date = None

        self.insert_patient(study.patientid, study.patient_name)

        existing_id = self.get_study_by_uid(study.study_uid)
        if existing_id is None:
            insert_study_query = """
            INSERT IGNORE INTO studies
            (id_patient,
            attachment_count, series_count, study_uid, uuid,
            study_description, updated, study_date, created_date,
            modality, phi_namespace, storage_namespace, viewer_link)
            VALUES ((SELECT patients.id FROM patients WHERE patients.patient_name=%s),
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            study_record = (study.patient_name,
                            study.attachment_count, len(list(study.get_series())), study.study_uid,
                            study.uuid, study.formatted_description, study_updated, study_date,
                            study.created, study.modality, study.phi_namespace,
                            study.storage_namespace, study.viewer_link)

            with self.connection.cursor() as cursor:
                cursor.execute(insert_study_query, study_record)
                self.connection.commit()
        else:
            update_study_query = """
            UPDATE studies SET
            attachment_count = %s,
            series_count = %s,
            uuid = %s,
            study_description = %s,
            updated = %s,
            study_date = %s,
            created_date = %s,
            phi_namespace = %s,
            storage_namespace = %s,
            viewer_link = %s,
            is_downloaded = NULL,
            download_date = NULL
            WHERE id = %s;
            """

            study_record = (study.attachment_count, len(list(study.get_series())),
                            study.uuid, study.formatted_description,
                            study_updated,
                            study_date,
                            study_created,
                            study.phi_namespace,
                            study.storage_namespace,
                            study.viewer_link,
                            existing_id)

            with self.connection.cursor() as cursor:
                cursor.execute(update_study_query, study_record)

            self.connection.commit()

        #self.add_to_sequence_map(study.formatted_description)

    # --------------------------------------------------------------------------
    def get_tag_value(self, tags, group_hex, element_hex):
        values = [this['value'] for this in tags['tags'] if this['group'] == int(str(group_hex),16) and this['element'] == int(str(element_hex), 16)]
        if len(values) == 0:
           return None
        elif len(values) == 1:
            return values[0]
        else:
            return values

    # --------------------------------------------------------------------------
    def insert_series(self, series):
        """
        Add series to the database.

        Inputs:
        -----------
        series: Object of the AMBRA_Utils.Series class
            Object of the series class to be added to the database.
        """
        insert_series_query = """
        INSERT IGNORE INTO img_series
        (id_study, scanner_model, scanner_manufac,
        magnetic_field_strength, device_serial_number, series_number, series_description,
        protocol_name, TR, TE, recon_matrix_rows,
        recon_matrix_cols, slice_thickness, number_of_slices,
        number_of_temporal_positions, acquisition_number, number_of_dicoms,
        series_uid, software_version, pixel_spacing,
        pixel_bandwidth, acq_matrix, perc_phase_fov,
        inversion_time, flip_angle, scanner_station_name, mr_acq_type, sequence_name)
        VALUES ((SELECT id from studies WHERE studies.study_uid=%s), %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, %s)
        """

        series_tags = series.get_tags(0)

        scanner_model = self.get_tag_value(series_tags, '0008', '1090')
        scanner_manufac = self.get_tag_value(series_tags, '0008', '0070')
        magnetic_field_strength = self.get_tag_value(series_tags, '0018', '0087')
        device_serial_number = self.get_tag_value(series_tags, '0018', '1000')
        series_number = self.get_tag_value(series_tags, '0020', '0011')
        series_description = series.formatted_description
        protocol_name = self.get_tag_value(series_tags, '0018', '1030')
        tr = self.get_tag_value(series_tags, '0018', '0080')
        te = self.get_tag_value(series_tags, '0018', '0081')
        recon_matrix_rows = self.get_tag_value(series_tags, '0028', '0010')
        recon_matrix_cols = self.get_tag_value(series_tags, '0028', '0011')
        slice_thickness = self.get_tag_value(series_tags, '0018', '0050')
        number_of_slices = self.get_tag_value(series_tags, '0054', '0081')
        number_of_temporal_positions = self.get_tag_value(series_tags, '0020', '0105')
        acquisition_number = self.get_tag_value(series_tags, '0020', '0012')
        number_of_dicoms = series.count
        series_uid = series.series_uid
        scanner_station_name = self.get_tag_value(series_tags, '0008', '1010')
        inversion_time = self.get_tag_value(series_tags, '0018', '0082')
        flip_angle = self.get_tag_value(series_tags, '0018', '1314')
        perc_phase_fov = self.get_tag_value(series_tags, '0018', '0094')
        acq_matrix = self.get_tag_value(series_tags, '0018', '1310')
        pixel_bandwidth = self.get_tag_value(series_tags, '0018', '0095')
        pixel_spacing = self.get_tag_value(series_tags, '0028', '0030')
        software_version = self.get_tag_value(series_tags, '0018', '1019')
        mr_acq_type = self.get_tag_value(series_tags, '0018','0023')
        seq_name = self.get_tag_value(series_tags, '0018','0024')

        series_record = (
            series.study.study_uid, scanner_model, scanner_manufac,
            magnetic_field_strength, device_serial_number, series_number, series_description,
            protocol_name, tr, te, recon_matrix_rows,
            recon_matrix_cols, slice_thickness, number_of_slices,
            number_of_temporal_positions, acquisition_number, number_of_dicoms,
            series_uid, software_version, pixel_spacing,
            pixel_bandwidth, acq_matrix, perc_phase_fov,
            inversion_time, flip_angle, scanner_station_name, mr_acq_type, seq_name
        )

        with self.connection.cursor() as cursor:
            cursor.execute(insert_series_query, series_record)

        self.connection.commit()

        self.add_to_series_map(series.formatted_description)

    # --------------------------------------------------------------------------
    def set_study_is_downloaded(self, study_uid, zip_path, nifti_dir, download_date):
        """
        Use paths relative to the backup directory.

        Inputs:
        -----------
        study_uid: str
            uid of the study to be marked as downloaded.
        zip_path: str or Path
            Path to the zip file relative to the backup directory.
        nifti_dir: str or Path
            Path to the directory containing the study nifti files.
        download_date: datetime Object
            Date and time the study was downloaded.
        """
        with self.connection.cursor() as cursor:
            download_query = """UPDATE studies SET is_downloaded = TRUE, zip_path = %s, nifti_directory = %s, download_date=%s WHERE study_uid=%s;"""
            cursor.execute(download_query, (str(zip_path), str(nifti_dir), download_date, study_uid))

        self.connection.commit()

    # --------------------------------------------------------------------------
    def study_download_date(self, study_uid):
        """
        Returns the download date if the study with the given study_uid has been marked as downloaded in the database.
        If the study is not marked as downloaded then 'None' will be returned.

        Inputs:
        -----------
        study_uid: str
            uid of the study to be marked as downloaded.
        """
        with self.connection.cursor() as cursor:
            #download_query = """UPDATE studies SET is_downloaded = TRUE, zip_path = %s, nifti_directory = %s, download_date=%s WHERE study_uid=%s;"""
            download_query = """SELECT is_downloaded, download_date FROM studies WHERE study_uid=%s;"""
            cursor.execute(download_query, (study_uid,))
            is_downloaded, download_date = cursor.fetchone()
        if is_downloaded:
            return download_date

        return is_downloaded

    # --------------------------------------------------------------------------
    def studies_not_downloaded(self):
        """
        Returns a list of study_uids from studies that have not been downloaded.
        """
        with self.connection.cursor(buffered=True) as cursor:
            download_query = """SELECT studies.uuid, studies.study_uid, studies.phi_namespace, backup_info.namespace_name
                                FROM studies INNER JOIN backup_info ON studies.phi_namespace = backup_info.namespace_id
                                WHERE studies.is_downloaded IS NULL OR studies.is_downloaded=FALSE;"""
            #download_query = """SELECT studies.id
            #                    FROM studies
            #                    WHERE studies.is_downloaded IS NULL OR studies.is_downloaded=FALSE;"""
            cursor.execute(download_query)
            while True:
                results = cursor.fetchmany(10)
                if not results:
                    break
                for result in results:
                    yield result

    # --------------------------------------------------------------------------
    def get_study_info_by_id_study(self, id_study):
        """
        Returns selected info for the study whose id in the studies table mathces id_study.
        """
        with self.connection.cursor() as cursor:
            download_query = """SELECT studies.uuid, studies.study_uid, studies.phi_namespace, backup_info.namespace_name
                                FROM studies INNER JOIN backup_info ON studies.phi_namespace = backup_info.namespace_id
                                WHERE studies.id=%s;"""

            cursor.execute(download_query, (id_study,))

            result = cursor.fetchone()
            return result



    # --------------------------------------------------------------------------
    def add_raw_nifti(self, nifti_path, series_uid):
        """
        Inserts the nifti_path into the column raw_nifti for the img_series table_name
        where series_uid matches the input paramter series_uid.

        Inputs:
        -----------
        nifti_path: str or Path
            Path to the nifti file.
        series_uid: str
            series_uid for the img_series belonging to the nifti file.
        """
        assert isinstance(series_uid, str)
        insert_query = """UPDATE img_series SET raw_nifti=%s WHERE series_uid=%s"""
        with self.connection.cursor() as cursor:
            cursor.executemany(insert_query, [(str(nifti_path), series_uid)])

        self.connection.commit()

    # --------------------------------------------------------------------------
    def add_niftis(self, nifti_dir):
        """
        Finds .nii and .nii.gz files in a given directory and assumes they contain the
        series uid in the file name. If so, will add them to the raw_nifti
        column of the img_series table in the database.

        Inputs:
        ----------
        nifti_dir: Path or str
            Path to the directory containing the nifti files.
        """
        # Add nifti files to img_series table
        for nifti_file in nifti_dir.glob('*.nii*'):
            nii_stem = nifti_file.stem
            if nii_stem.endswith('.nii'):
                nii_stem = nii_stem[0:-4]
            series_uid = nii_stem.split('_')[-1]

            #TODO: This is very basic check if uid was found, could be better
            if len(series_uid) < 10:
                series_uid = nii_stem.split('_')[-2]
            if len(series_uid) < 10:
                logging.warning(f'Could not find uid for {nifti_file}.')
                continue

            try:
                self.add_raw_nifti(nifti_file, series_uid)
            except:
                logging.warning(f'Could not add {nifti_file} to database.')

    # --------------------------------------------------------------------------
    def add_nifti_paths(self, backup_path, nifti_directory, series):
        """
        Searches for nifti files matching the series description and series number
        and adds them to the raw_nifti field in the img_series table.

        Inputs:
        -----------
        backup_path: str or Path
            Path to the backup directory.
        nifti_directory: str or Path
            Path to the directory containing the nifti files.
        series: Object of AMBRA_Utils.Series class.

        """
        nifti_dir = Path(nifti_directory)
        series_tags = series.get_tags(0)
        series_number = self.get_tag_value(series_tags, '0020', '0011')
        search_pattern = f'{series.formatted_description}_*_{series_number}.nii*'

        niftis = list(nifti_dir.glob(search_pattern))
        if len(niftis) == 1:
            nifti = niftis[0]
            self.add_raw_nifti(nifti.relative_to(backup_path), series.series_uid)

        elif len(niftis) > 1:
            logging.warning(f"Multiple nifti files found matching the pattern {search_pattern}, no path added to database!")
        elif len(niftis) == 0:
            logging.warning(f"No nifti files found matching the pattern {search_pattern}, no path added to database!")

        # bvals_search_pattern = f'{series.formatted_description}_*_{series_number}.bval'
        # bvecs_search_pattern = f'{series.formatted_description}_*_{series_number}.bvec'
        #
        # bvals = list(nifti_dir.glob(bvals_search_pattern))
        # if len(bvals) == 1:
        #     bval = bvals[0]
        #     insert_query = """UPDATE img_series SET bvals=%s WHERE series_uid=%s"""
        #     with self.connection.cursor() as cursor:
        #         cursor.executemany(insert_query, [(str(bval.relative_to(backup_path)), series.series_uid)])
        #
        # bvecs = list(nifti_dir.glob(bvecs_search_pattern))
        # if len(bvecs) == 1:
        #     bvec = bvecs[0]
        #     insert_query = """UPDATE img_series SET bvecs=%s WHERE series_uid=%s"""
        #     with self.connection.cursor() as cursor:
        #         cursor.executemany(insert_query, [(str(bvec.relative_to(backup_path)), series.series_uid)])

    # --------------------------------------------------------------------------
    def get_id_series_name(self, series_description):
        """
        Returns the id from the series_name table where corresponding to the
        mapping in the series_map table.
        """
        res = list(self.run_select_query(f"SELECT id_series_name FROM series_map WHERE series_description=LOWER('{series_description}');"))
        if res == []:
            return None
        else:
            return res[0][0]

    # --------------------------------------------------------------------------
    def set_id_series_names(self):
        """
        Updates the id_series_name column of the img_series tables using the mapping in the series_map table.
        """
        null_id_series_names = self.run_select_query("SELECT id, series_description FROM img_series WHERE id_series_name is null;")

        for this_series in null_id_series_names:
            id_img, series_desc = this_series
            id_series_name = self.get_id_series_name(series_desc)
            if id_series_name:
                self.run_insert_query("UPDATE img_series SET id_series_name=%s WHERE id=%s", (id_series_name, id_img))

    # --------------------------------------------------------------------------
    def add_to_series_map(self, series_description):
        """
        Inserts the series_description into the series_description column of the
        series_map table. Duplicate entries are ignored.
        """
        query = "INSERT IGNORE INTO series_map (series_description) VALUES (LOWER(%s))"
        self.run_insert_query(query, (series_description,))

    # --------------------------------------------------------------------------
    def add_image_to_processing(self, id_img_series, image_path):
        """
        """
        pass
        #image_path = Path(image_path); assert image_path.exists()
        ## XXX:
        # Need to hash image and insert into processing table

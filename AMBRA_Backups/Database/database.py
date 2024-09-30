import os
from pathlib import Path
import logging
import pdb
from datetime import datetime
from mysql.connector import connect, Error, FieldType
import mysql.connector.errors as mysql_errors
import configparser
from string import Template
import hashlib
import json
import nibabel as nib
import pandas as pd
from time import sleep

from AMBRA_Utils import Series

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
    def close(self):
        self.connection.commit()
        self.connection.close()

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
    def connect(cls, db_name=None, config_path=None, n_retries=5):
        """
        """
        config = cls.get_config(config_path=config_path)
        db_config = config['ambra_backup']

        retry_num = 0
        while retry_num < n_retries:
            try:
                connection = connect(
                    host = db_config['host'],
                    port = db_config['port'],
                    user = db_config['user_name'],
                    password = db_config['password'],
                    database = db_name,
                    #pool_size = 500
                    buffered=True,
                    #consume_results=True
                )

                return connection
            except mysql_errors.DatabaseError:
                retry_num += 1
                sleep(5)
            except Exception as e:
                logging.error(e)
                raise e

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
    def run_select_query(self, query, record=None, column_names=False, buffered=True, field_types=False):
        """
        Runs an SQL SELECT query and return the results.

        Do not set buffered to True if you expect a large result to be returned.

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
            if record:
                cursor.execute(query, record)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            columns = cursor.description

        self.connection.commit()

        these_field_types = {this[0]:FieldType.get_info(this[1]) for this in columns}

        if column_names:
            result_dicts = [{columns[index][0]:column for index, column in enumerate(value)} for value in results]
            if field_types:
                return result_dicts, these_field_types   
            return result_dicts
        
        if field_types:
            return list(results), these_field_types
        return list(results)

    # --------------------------------------------------------------------------
    def run_insert_query(self, query, record):
        """
        Runs an SQL INSERT/UPDATE query.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, record)
            row_id = cursor.lastrowid

        self.connection.commit()
        return row_id

    # --------------------------------------------------------------------------
    def insert_dict(self, dict, table):
        """
        Insert the dictionary into the specified table with keys being the
        column names.

        Returns the id of the inserted row.
        """
        query = f"INSERT INTO {table} ( " + ", ".join(dict.keys()) + ") " + \
                "VALUES ( " + ", ".join(["%s" for this in dict.values()]) + " );"

        row_id = self.run_insert_query(query, tuple(dict.values()))
        return row_id

    # --------------------------------------------------------------------------
    def update_dict(self, dict, table, id_column, id_value):
        """
        Update the dictionary into the specified table with keys being the column
        names for the 'id_column' row with value 'id_value'.
        """
        set_string = ', '.join([str(this)+'=%s' for this in dict.keys()])
        query = f"UPDATE {table} SET {set_string} WHERE {id_column}='{id_value}';"

        row_id = self.run_insert_query(query, tuple(dict.values()))
        return row_id

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
        with self.connection.cursor(buffered=True) as cursor:
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
    def get_study_by_uid(self, uid, storage_ns=None):
        """
        Returns the id from the studies table for the study that matches the
        given uid.

        Inputs:
        ----------
        uid: str
            uid of the study to retrieve.
        storage_ns: str, None
            If not None, will return study matching uid and storage namespace ID.
        """
        select_query = """SELECT id FROM studies WHERE studies.study_uid=%s"""
        if storage_ns is not None:
            select_query += """ AND storage_namespace=%s"""
            with self.connection.cursor() as cursor:
                cursor.execute(select_query, (uid, storage_ns))
                result = cursor.fetchone()
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(select_query, (uid, ))
                result = cursor.fetchone()

        if result is None:
            return None
        return result[0]
    
    # --------------------------------------------------------------------------
    def get_study_by_uuid(self, uuid, storage_ns=None):
        """
        Returns the id from the studies table for the study that matches the
        given uuid.

        Inputs:
        ----------
        uid: str
            uuid of the study to retrieve.
        storage_ns: str, None
            If not None, will return study matching uuid and storage namespace ID.
        """
        select_query = """SELECT id FROM studies WHERE studies.uuid=%s"""
        if storage_ns is not None:
            select_query += """ AND storage_namespace=%s"""
            with self.connection.cursor() as cursor:
                cursor.execute(select_query, (uuid, storage_ns))
                result = cursor.fetchone()
        else:
            with self.connection.cursor() as cursor:
                cursor.execute(select_query, (uuid, ))
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
    def insert_study(self, study, custom_fields=None, custom_functions=None, redownload=True, ignore_existing=False):
        """
        Because study_uid is set as a unique primary key and IGNORE is used in the query,
        if the study is already in the database nothing will happen.

        Inputs:
        -----------
        study: Object of the AMBRA_Utils.Study class
            Object of the study class to be added to the database.
        custom_fields: dict
            The key should contain the name of the custom field in Ambra and the
            value the name of the column in the database.
        custom_funtions: dict
            The key should contain the name of the column in the database and
            the value contains the function that will be run with the study
            passed on the parameter.
        redownload: bool
            If true, then a study update will null the is_downloaded and download_date fields.
            If false, then those fields will be left as is.
        """

        if study.created[-3:] == '-07':
            created_string = study.created[0:-3]
        else:
            created_string = study.created

        if '+' in created_string:
            created_string = created_string.split('+')[0]

        try:
            study_created = datetime.strptime(created_string, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            try:
                study_created = datetime.strptime(created_string, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                study_created = None

        if not study.updated:
            study_updated = study_created
        else:
            if study.updated[-3:] == '-07':
                updated_string = study.updated[0:-3]
            else:
                updated_string = study.updated
            try:
                study_updated = datetime.strptime(updated_string, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    study_updated = datetime.strptime(updated_string, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    study_updated = None


        #print(f'Study date: {study.study_date}')
        if study.study_date:
            # If time not in study date add it in through the study_time parameter.
            this_study_date = study.study_date

            if '-' in this_study_date:
                date_format = '%Y-%m-%d'
            elif '/' in this_study_date:
                date_format = '%m/%d/%Y'
            else:
                date_format = '%Y%m%d'

            if study.study_time is None:
                datetime_format = date_format
            else:
                if len(this_study_date) <= 10:
                    this_study_date = f'{study.study_date} {study.study_time}'

                if ':' in this_study_date:
                    time_format = '%H:%M:%S'
                else:
                    time_format = '%H%M%S'

                if '.' in this_study_date:
                    time_format += '.%f'

                datetime_format = f'{date_format} {time_format}'

            try:
                study_date = datetime.strptime(this_study_date, datetime_format)
            except ValueError:
                study_date = None

            # try:
            #     study_date = datetime.strptime(study.study_date, '%Y-%m-%d %H:%M:%S.%f')
            # except ValueError:
            #     try:
            #         study_date = datetime.strptime(study.study_date, '%Y-%m-%d %H:%M:%S')
            #     except ValueError:
                    
                    

            #         try:
            #             try:
            #                 study_date = datetime.strptime(f'{this_study_date} {this_study_time}', '%Y%m%d %H%M%S.%f')
            #             except ValueError:
            #                 study_date = datetime.strptime(f'{this_study_date} {this_study_time}', '%Y%m%d %H%M%S')
            #         except:
            #             try:
            #                 study_date = datetime.strptime(study.study_date, '%Y-%m-%d')
            #             except ValueError:
            #                 try:
            #                     study_date = datetime.strptime(study.study_date, '%Y%m%d')
            #                 except ValueError:
            #                     try:
            #                         study_date = datetime.strptime(study.study_date, '%m/%d/%Y')
            #                     except:
            #                         try:
            #                             study_date = datetime.strptime(study.study_date, '%d/%m/%Y')
            #                         except:
            #                             study_date = None
        else:
            study_date = None

        cfields_values = []
        cfields_dbcols = []
        if custom_fields:
            for custom_field in custom_fields.keys():
                try:
                    cfield_value = study.get_customfield_value(custom_field)

                    cfields_values.append(cfield_value)
                    cfields_dbcols.append(custom_fields[custom_field])
                except:
                    continue

        if custom_functions:
            for custom_dbcol in custom_functions.keys():
                try:
                    cfield_value = custom_functions[custom_dbcol](study)

                    cfields_values.append(cfield_value)
                    cfields_dbcols.append(custom_dbcol)
                except:
                    continue

        def add_comma(this_list):
            if len(this_list) > 0:
                return ', '
            return ''


        self.insert_patient(study.patientid, study.patient_name)

        existing_id = self.get_study_by_uid(study.study_uid)
        if ( existing_id is None ) or ignore_existing:

            if study.patient_name is None or study.patient_name == '':
                raise Exception("Error: Patient name is empty!")

            insert_study_query = f"""
            INSERT IGNORE INTO studies
            (id_patient,
            attachment_count, series_count, study_uid, uuid,
            study_description, updated, study_date, created_date,
            modality, phi_namespace, storage_namespace, viewer_link, must_approve {add_comma(cfields_dbcols) + ', '.join(cfields_dbcols)})
            VALUES ((SELECT patients.id FROM patients WHERE patients.patient_name=%s),
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s {len(cfields_values)*', %s'})
            """

            study_record = (study.patient_name,
                            study.attachment_count, len(list(study.get_series())), study.study_uid,
                            study.uuid, study.formatted_description, study_updated, study_date,
                            study.created, study.modality, study.phi_namespace,
                            study.storage_namespace, study.viewer_link, study.must_approve) + tuple(cfields_values)

            with self.connection.cursor() as cursor:
                cursor.execute(insert_study_query, study_record)
                self.connection.commit()
        else:
            def set_download(download):
                if download:
                    return ", is_downloaded = NULL, download_date = NULL"
                else:
                    return ""

            update_study_query = f"""
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
            viewer_link = %s {set_download(redownload)},
            must_approve = %s,
            deleted = 0
            {add_comma(cfields_dbcols) + ', '.join([this+" = %s" for this in cfields_dbcols])}
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
                            study.must_approve) + tuple(cfields_values) + (existing_id, )

            with self.connection.cursor() as cursor:
                cursor.execute(update_study_query, study_record)

            self.connection.commit()

        #self.add_to_sequence_map(study.formatted_description)

        # Add study tags
        id_study = self.get_study_by_uid(study.study_uid, storage_ns=study.storage_namespace)
      
        if id_study is not None:
            for tag in study.get_study_tags()['tags']:
                group, element = tag['tag'].strip('(').strip(')').split(',')
                value = tag['value']
                max_tag_value_length = 512
                if len(value) > max_tag_value_length:
                    value = value[0:512]
                tag_query = """INSERT INTO study_tags (id_study, tag_group, tag_element, tag_value) 
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                            tag_value = %s"""
                tag_record = (id_study, group, element, value, value)

                with self.connection.cursor() as cursor:
                    cursor.execute(tag_query, tag_record)

            self.connection.commit()

    # --------------------------------------------------------------------------
    def get_tag_value(self, tags, group_hex, element_hex):
        if tags is None:
            return None
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

        #try:
        series_tags = series.get_tags(0)
        #except:
        #    series_tags = None

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
        software_version = self.get_tag_value(series_tags, '0018', '1020')
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
    def set_study_is_downloaded(self, study_uid, zip_path, nifti_dir, download_date, is_downloaded=True, uuid=None):
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
        is_downloaded: bool
            The value to set is_downloaded in the database. Default: True
        uuid: str
            If the uuid is not None, then this will also be used in the SQL query to identify the study.
        """
        if zip_path is not None:
            zip_path = str(zip_path)
        if nifti_dir is not None:
            nifti_dir = str(nifti_dir)
        if is_downloaded is not None:
            is_downloaded = bool(is_downloaded)

        with self.connection.cursor() as cursor:
            download_query = """UPDATE studies SET is_downloaded = %s, zip_path = %s, nifti_directory = %s, download_date=%s WHERE study_uid=%s"""
            records = (is_downloaded, zip_path, nifti_dir, download_date, study_uid)
            if uuid is not None:
                download_query += """ AND uuid=%s"""
                records = (is_downloaded, zip_path, nifti_dir, download_date, study_uid, uuid)
            cursor.execute(download_query, records)

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
            download_query = """SELECT studies.uuid, studies.study_uid, studies.phi_namespace, backup_info.namespace_name, studies.id
                                FROM studies INNER JOIN backup_info ON studies.phi_namespace = backup_info.namespace_id
                                WHERE (studies.is_downloaded IS NULL OR studies.is_downloaded=FALSE)
                                AND (studies.deleted != 1 OR studies.deleted is NULL);"""
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
    def to_float(self, value):
        try:
            return float(value)
        except ValueError:
            return None
        except TypeError:
            return None

    # --------------------------------------------------------------------------
    def to_int(self, value):
        try:
            return float(value)
        except ValueError:
            return None
        except TypeError:
            return None

    # --------------------------------------------------------------------------
    def add_area_annotations(self, annotations_json, id_study):
        """
        Adds area annotations stored in the annotations_json file to the
        area_annotations table in the database.

        Annotation will be added to the annotations table through a call to
        add_annotations regardless of whether it's an area annotation or not.
        """
        id_annot = self.add_annotations(annotations_json, id_study)

        with open(annotations_json, 'r') as fopen:
            annots = json.load(fopen)

        for annot in annots:
            annot_json = json.loads(annot.pop('json'))
            if annot_json['type'] == 'Area':
                stats = annot_json['stats']
                annot_area_info = {
                  'id_annotations': id_annot,
                  'instance_uid': annot.get('instance_uid'),
                  'stamp': annot.get('stamp'),
                  'frame_number': int(annot.get('frame_number')),
                  'user_name': annot.get('user_name'),
                  'series_uid': annot.get('series_uid'),
                  'user_id': annot.get('user_id'),
                  'uuid': annot.get('uuid'),
                  'type': annot_json.get('type'),
                  'area': annot_json.get('area'),
                  'color': int(annot_json.get('color')),
                  'filled': bool(annot_json.get('filled')),
                  'height': int(annot_json.get('height')),
                  'width': int(annot_json.get('width')),
                  'stats_count': self.to_int(stats.get('count')),
                  'stats_max': self.to_float(stats.get('max')),
                  'stats_min': self.to_float(stats.get('min')),
                  'stats_mean': self.to_float(stats.get('mean')),
                  'stats_stdev': self.to_float(stats.get('stdev')),
                  'stats_sum': self.to_float(stats.get('sum')),
                  'stats_pixelSpacing': self.to_float(stats.get('pixelSpacing')),
                  'description': annot_json.get('description'),
                  'instanceIndex': int(annot_json.get('instanceIndex')),
                }
                try:
                    self.insert_dict(annot_area_info, 'area_annotations')
                except mysql_errors.IntegrityError:
                    res = self.run_select_query('SELECT id FROM area_annotations WHERE uuid = %s', (annot_area_info['uuid'],))
                    if len(res) == 1:
                        id_value = res[0][0]
                        id_annot = self.update_dict(annot_area_info, 'area_annotations', 'id', id_value)
                    else:
                        raise Exception('Could not add annotation file.')

                #XXX: This will fail if the annotation uuid is not unique -
                # need to implement an update if it's already in the database

    # --------------------------------------------------------------------------
    def add_annotations(self, annotations_json, id_study):
        """
        Adds annotations stored in the annotations_json file to the annotations database.

        Returns the id of the entry from the annotations table.
        """
        assert isinstance(id_study, int)
        annot_info = {
            'id_study': id_study,
            'file_path': str(annotations_json)
        }

        try:
            id_annot = self.insert_dict(annot_info, 'annotations')
        except mysql_errors.IntegrityError:
            # Most likely thrown if row already exists.
            res = self.run_select_query('SELECT id FROM annotations WHERE file_path = %s', (str(annotations_json),))
            if len(res) == 1:
                id_value = res[0][0]
                id_annot = self.update_dict(annot_info, 'annotations', 'id', id_value)
            else:
                raise Exception('Could not add annotation file.')

        return id_annot

    # --------------------------------------------------------------------------
    def get_id_series_name(self, series_description):
        """
        Returns the id from the series_name table where corresponding to the
        mapping in the series_map table.
        """
        res = list(self.run_select_query(f"""SELECT id_series_name FROM series_map WHERE series_description=LOWER(%s);""", record=(series_description,)))
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

    # ------------------------------------------------------------------------------
    def hash_file(self, file_path):
        """
        Returns the md5 hash of the file at file_path.
        """
        file_path = Path(file_path)
        if not file_path.is_file():
            raise Exception('Only files can be hashed.')

        hasher = hashlib.md5()
        with open(file_path, 'rb') as fopen:
            buf = fopen.read()
            hasher.update(buf)

        return hasher.hexdigest()

    # ------------------------------------------------------------------------------
    def add_nifti(self, nifti_path, json_path=None, id_img_series=None, id_study=None):
        """
        Adds the nifti file at nifti_path and information from the json file to the
        database.  If id_img_series and/or id_study is entered then the row will be
        linked to those databases using their foreign keys.

        Raises mysql.connector.errors.DataError if the file is already in the database.
        """
        if not Path(nifti_path).exists():
            raise Exception(f'The file at {nifti_path} does not exist!')

        info = {'file_path':str(nifti_path)}
        if json_path:
            try:
                with open(json_path, 'r') as fopen:
                    data = json.load(fopen)
            except json.decoder.JSONDecodeError as e:
                raise Exception(f"Error loading the json file {json_path}.")

            info['json_path'] = str(json_path)
            info['Modality'] = data.get('Modality')
            info['Manufacturer'] = data.get('Manufacturer')
            info['ManufacturersModelName'] = data.get('ManufacturersModelName')
            info['BodyPartExamined'] = data.get('BodyPartExamined')
            info['PatientPosition'] = data.get('PatientPosition')
            info['ProcedureStepDescription'] = data.get('ProcedureStepDescription')
            info['SoftwareVersions'] = data.get('SoftwareVersions')
            info['SeriesDescription'] = data.get('SeriesDescription')
            info['ProtocolName'] = data.get('ProtocolName')

            image_types = data.get('ImageType')
            if isinstance(image_types, list):
                info['ImageType'] = ';'.join(image_types)
            else:
                info['ImageType'] = str(image_types)

            info['RawImage'] = data.get('RawImage')
            info['SeriesNumber'] = data.get('SeriesNumber')
            info['AcquisitionTime'] = data.get('AcquisitionTime')
            info['AcquisitionNumber'] = data.get('AcquisitionNumber')
            info['ConversionSoftware'] = data.get('ConversionSoftware')
            info['ConversionSoftwareVersion'] = data.get('ConversionSoftwareVersion')

        img = nib.load(nifti_path)
        info['xdim'] = int(img.header['dim'][1])
        info['ydim'] = int(img.header['dim'][2])
        info['zdim'] = int(img.header['dim'][3])
        info['tdim'] = int(img.header['dim'][4])

        if id_img_series:
            info['id_img_series'] = id_img_series
        if id_study:
            info['id_study'] = id_study

        info['md5_hash'] = self.hash_file(nifti_path)

        self.insert_dict(info, 'nifti_data')

    # ------------------------------------------------------------------------------
    def get_study_id(self, nifti_dir):
        assert Path(nifti_dir).exists()
        results = self.run_select_query("SELECT * FROM studies WHERE nifti_directory=%s", (str(nifti_dir),), column_names=True)
        if len(results)!=1:
            return None
        return results[0]['id']

    # ------------------------------------------------------------------------------
    def get_img_series_id(self, nii_path, json_path):
        """

        """
        nii_path = Path(nii_path)
        assert nii_path.exists()
        json_path = Path(json_path)
        assert json_path.exists()

        with open(json_path, 'r') as fopen:
            json_data = json.load(fopen)
        formatted_json_description = Series.Series.format_description(json_data['SeriesDescription'])
        series_number = json_data['SeriesNumber']

        nii_dir = nii_path.parent
        assert nii_dir.exists()

        result = self.run_select_query("""SELECT img_series.id from img_series
                                INNER JOIN studies ON img_series.id_study = studies.id
                                WHERE img_series.series_number = %s
                                AND img_series.series_description = %s
                                AND studies.nifti_directory = %s""", (series_number, formatted_json_description, str(nii_dir)))
        if len(result) == 0:
            return None
        elif len(result) > 1:
            raise Exception('Multiple images found!')
        return result[0][0]

    # ------------------------------------------------------------------------------
    def add_nifti_dir(self, nifti_dir):
        """
        Loops over all *.nii.gz files in the directory and calls add_nifti.
        """
        id_study = self.get_study_id(nifti_dir)
        for nifti_file in nifti_dir.glob('*.nii.gz'):
            json_file = nifti_dir.joinpath(nifti_file.name.replace('.nii.gz', '.json'))
            if not json_file.exists():
                json_file = None
                id_img_series = None
            else:
                try:
                    id_img_series = self.get_img_series_id(nifti_file, json_file)
                except:
                    id_img_series = None

            try:
                self.add_nifti(nifti_file, json_file, id_img_series=id_img_series, id_study=id_study)
            except mysql_errors.IntegrityError as e:
                # Most likely thrown if row already exists.
                print(f'Could not add nifti: {nifti_file}', e)
                continue
            except:
                print(f'Could not add nifti: {nifti_file}')
                continue

    # ------------------------------------------------------------------------------
    def add_niftis_in_study_dir(self, study_dir):
        """
        Searches the study backup directory for a directory named like *_nii and
        calls add_nifti_dir.

        """
        study_name = study_dir.name
        for nifti_dir in study_dir.glob('*_nii'):
            self.add_nifti_dir(nifti_dir)

import os
import configparser
from pathlib import Path
import logging
from datetime import datetime
from itertools import chain
import pdb

import mysql.connector.errors as mysql_errors
from ambra_sdk.exceptions.storage import NotFound

from AMBRA_Backups import utils
from AMBRA_Utils import Api, utilities

# ------------------------------------------------------------------------------
def get_zip_stem(study):
    zip_stem = study.formatted_description
    if zip_stem == '':
        zip_stem = f'{study.modality}_{study.study_date}'
    if zip_stem[0] == '.':
        zip_stem.replace('.', '_')

    return zip_stem

# ------------------------------------------------------------------------------
def backup_study(study, backup_path, convert=False, use_uid=False, force=False):
    """
    Backup the given study to the backup_path.

    Inputs:
    -------
    study:

    backup_path: Path object
        Path object to the main backup directory.

    convert: If True, will convert the dicoms to nifti and put them in a directory
        called *_nii

    use_uid: bool
        If True, will include the study uid in the data directory name.

    force: bool
        If True, will download the study regardless of whether a zip file currently exists.
    """
    if use_uid:
        uid_string = study.study_uid.replace('.', '_')
        study_dir = backup_path.joinpath(f'{study.patient_name}', f'{study.modality}_{uid_string}_{study.study_date}')
    else:
        study_dir = backup_path.joinpath(f'{study.patient_name}', f'{study.modality}_{study.study_date}')
    if not study_dir.exists():
        os.makedirs(study_dir)

    zip_stem = get_zip_stem(study)
    zip_file = study_dir.joinpath(f'{zip_stem}.zip')
    logging.info(f'\tPatient Name: {study.patient_name}\n\tPatient ID: {study.patientid}\n\tStudy date: {study.study_date}\n\tCreated: {study.created}\n\tUpdated: {study.updated}')

    if (not zip_file.exists()) or force:
        logging.info(f'\tBacking up {study.patient_name} to {zip_file}.')
        study.download(zip_file, ignore_exists=True)
    else:
        logging.info(f'\tSkipping backup of {study.patient_name} {study.formatted_description}, zip file already exists.')

    if convert:
        nifti_dir = study_dir.joinpath(f'{zip_stem}_nii')
        if (not nifti_dir.exists()) or force:
            try:
                utils.extract_and_convert(zip_file, nifti_dir, cleanup=True)
            except Exception as e:
                logging.error(e)

        return zip_file, nifti_dir

    return zip_file, None

# ------------------------------------------------------------------------------
def backup_namespace(namespace, backup_path, min_date=None, convert=False, use_uid=False):
    """
    Backup all subject data belonging to the input namespace. If min_date is set
    then only subject data that has been updated after that date will be downloaded.

    Inputs:
    --------
    namespace: Object of the Api.Namespace class or one of its subclasses.

    backup_path: String, Path; Path to where the data will be stored. Must exist.

    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.

    convert: If True, will convert the dicoms to nifti and put them in a directory
        called *_nii

    database: Database object
        Object of the AMBRA_Backups.database.Database class.
    """
    assert isinstance(namespace, Api.Namespace)
    backup_log = backup_path.joinpath('backups.log')
    logging.basicConfig(filename=backup_log, format='%(levelname)s: %(asctime)s: %(message)s', level=logging.INFO)

    logging.info(f'Backing up studies for {namespace}.')
    if isinstance(min_date, datetime):
        studies_to_backup = namespace.get_studies_after(min_date, updated=True)
    else:
        studies_to_backup = namespace.get_studies()

    for study in studies_to_backup:
        try:
            backup_study(study, backup_path, convert=convert, use_uid=use_uid)
        except Exception as e:
            print(e)
            logging.error(e)

# ------------------------------------------------------------------------------
def backup_account(account_name, backup_path, min_date=None, groups=True, locations=False, convert=False, use_uid=False):
    """
    Inputs:
    -------
    account_name: String; Name of the account to backup.

    backup_path: String, Path; Path to where the data will be stored. Must exist.

    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.

    groups: bool; If True then all account groups will be backed up.

    locations: bool; If True then all account locations will be backed up.

    convert: If True, will convert the dicoms to nifti and put them in a directory
        called *_nii

    database: Database object
        Object of the AMBRA_Backups.database.Database class.
    """
    backup_path = Path(backup_path)
    assert backup_path.exists()

    backup_log = backup_path.joinpath('backups.log')
    logging.basicConfig(filename=backup_log, format='%(levelname)s: %(asctime)s: %(message)s', level=logging.INFO)
    logging.info(f'Backing up studies for account {account_name}.')

    ambra = utilities.get_api()
    account = ambra.get_account_by_name(account_name)

    if groups:
        logging.info(f'Backing up all groups for account {account_name}.')
        for group in account.get_groups():
            print(20*'=' + f'\n{group}\n' + 20*'=')
            backup_namespace(group, backup_path, min_date=min_date, convert=convert, use_uid=use_uid)
    if locations:
        logging.info(f'Backing up all locations for account {account_name}.')
        for location in account.get_locations():
            print(location)
            backup_namespace(location, backup_path, min_date=min_date, convert=convert, use_uid=use_uid)

# ------------------------------------------------------------------------------
def update_database(database, namespace, custom_fields=None, custom_functions=None):
    """
    Inputs:
    -------
    database: Object of the Database class

    namespace: Object of the Namespace class or one of it's subclasses.

    custom_fields: dict
        The key should contain the name of the custom field in Ambra and the
        value the name of the column in the database. This gets passed to the
        same field in the database.insert_study() method.

    custom_funtions: dict
        The key should contain the name of the column in the database and
        the value contains the function that will be run with the study
        passed on the parameter. This gets passed to the same field in the
        database.insert_study() method.
    """
    last_backup = database.get_last_backup(namespace.name, namespace.namespace_type)
    current_backup = datetime.now()
    if last_backup is None:
        studies = namespace.get_studies()
    else:
        # This is a fix for an Ambra bug that is setting the study 'updated' field to null
        # on newly inserted studies, should only need the method with 'updated=True'  - TCM 02/28/2022
        studies = chain(namespace.get_studies_after(last_backup, updated=True),
                        namespace.get_studies_after(last_backup, updated=False))
    for study in studies:
        print(study)
        try:
            database.insert_study(study, custom_fields=custom_fields, custom_functions=custom_functions)
            series = study.get_series()
            for this_series in series:
                database.insert_series(this_series)
        except mysql_errors.ProgrammingError as e:
            raise Exception(e)
        except NotFound:
            print(f'Could not find the study {study.patient_name}: {study.uuid}.')
        except Exception as e:
            print('Error inserting study into database.')
            print(e)

    database.insert_update_datetime(namespace.name, namespace.namespace_type, namespace.namespace_id, namespace.uuid, current_backup)

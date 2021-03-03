import os
import configparser
from pathlib import Path
import logging
from datetime import datetime
import pdb
import utils

from AMBRA_Utils import Api

# ------------------------------------------------------------------------------
def get_api(config_path=None):
    """
    Gets user credentials from config file with the following format:
      [ambra]
      user_name = XXX
      password = XXX

    If the config_path input is None, it will look for the file ~/.ambra_credentials

    Returns an object of the AMBRA_Utils.Api class
    """
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = Path.home().joinpath('.ambra_credentials')
    if not config_file.exists():
        logging.error(f'Could not find the credentials file: {config_file}')

    config = configparser.ConfigParser()
    config.read(config_file)

    ambra = Api.Api(config['ambra']['user_name'], config['ambra']['password'])
    return ambra

# ------------------------------------------------------------------------------
def backup_namespace(namespace, backup_path, min_date=None, convert=False):
    """
    Backup all subject data belonging to the input namespace. If min_date is set
    then only subject data that has been updated after that date will be downloaded.

    namespace: Object of the Api.Namespace class or one of its subclasses.
    backup_path: String, Path; Path to where the data will be stored. Must exist.
    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.
    convert: If True, will convert the dicoms to nifti and put them in a directory
        called *_nii
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
        study_dir = backup_path.joinpath(f'{study.patient_name}', f'{study.modality}_{study.study_date}')
        if not study_dir.exists():
            os.makedirs(study_dir)

        zip_stem = study.formatted_description
        if zip_stem[0] == '.':
            zip_stem.replace('.', '_')
        zip_file = study_dir.joinpath(f'{zip_stem}.zip')
        logging.info(f'\tPatient Name: {study.patient_name}\n\tPatient ID: {study.patientid}\n\tStudy date: {study.study_date}\n\tCreated: {study.created}\n\tUpdated: {study.updated}')

        if not zip_file.exists():
            logging.info(f'\tBacking up {study.patient_name} to {zip_file}.')
            study.download(zip_file)
        else:
            logging.info(f'\tSkipping backup of {study.patient_name} {study.formatted_description}, zip file already exists.')

        if convert:
            nifti_dir = study_dir.joinpath(f'{zip_stem}_nii')
            if not nifti_dir.exists():
                try:
                    utils.extract_and_convert(zip_file, nifti_dir, cleanup=True)
                except Exception as e:
                    logging.error(e)

# ------------------------------------------------------------------------------
def backup_account(account_name, backup_path, min_date=None, groups=True, locations=False, convert=False):
    """

    account_name: String; Name of the account to backup.
    backup_path: String, Path; Path to where the data will be stored. Must exist.
    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.
    groups: bool; If True then all account groups will be backed up.
    locations: bool; If True then all account locations will be backed up.
    convert: If True, will convert the dicoms to nifti and put them in a directory
        called *_nii
    """
    backup_path = Path(backup_path)
    assert backup_path.exists()

    backup_log = backup_path.joinpath('backups.log')
    logging.basicConfig(filename=backup_log, format='%(levelname)s: %(asctime)s: %(message)s', level=logging.INFO)
    logging.info(f'Backing up studies for account {account_name}.')

    ambra = get_api()
    account = ambra.get_account_by_name(account_name)

    if groups:
        logging.info(f'Backing up all groups for account {account_name}.')
        for group in account.get_groups():
            print(20*'=' + f'\n{group}\n' + 20*'=')
            backup_namespace(group, backup_path, min_date=min_date, convert=convert)
    if locations:
        logging.info(f'Backing up all locations for account {account_name}.')
        for location in account.get_locations():
            print(location)
            backup_namespace(location, backup_path, min_date=min_date, convert=convert)

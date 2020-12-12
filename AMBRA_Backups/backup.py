import os
import configparser
from pathlib import Path
import logging
from datetime import datetime
import pdb

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
def backup_namespace(namespace, backup_path, min_date=None):
    """
    Backup all subject data belonging to the input namespace. If min_date is set
    then only subject data that has been updated after that date will be downloaded.

    namespace: Object of the Api.Namespace class or one of its subclasses.
    backup_path: String, Path; Path to where the data will be stored. Must exist.
    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.
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
        zip_file = study_dir.joinpath(f'{study.formatted_description}.zip')
        print(f'\tPatient Name: {study.patient_name}\n\tPatient ID: {study.patientid}\n\tStudy date: {study.study_date}\n\tCreated: {study.created}\n\tUpdated: {study.updated}')
        print(f'\tDownloading to: {zip_file}')

        if not zip_file.exists():
            logging.info(f'\tBacking up {study.patient_name} to {zip_file}.')
            study.download(zip_file)
            #pdb.set_trace()
        else:
            logging.info(f'\tSkipping backup of {study.patient_name} {study.formatted_description}, zip file already exists.')
        print(20*'-')

# ------------------------------------------------------------------------------
def backup_account(account_name, backup_path, min_date=None, groups=True, locations=False):
    """

    account_name: String; Name of the account to backup.
    backup_path: String, Path; Path to where the data will be stored. Must exist.
    min_date: datetime object; If not None then will backup all studies updated
        after the specified date.
    groups: bool; If True then all account groups will be backed up.
    locations: bool; If True then all account locations will be backed up.
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
            backup_namespace(group, backup_path, min_date=min_date)
    if locations:
        logging.info(f'Backing up all locations for account {account_name}.')
        for location in account.get_locations():
            print(location)
            backup_namespace(location, backup_path, min_date=min_date)

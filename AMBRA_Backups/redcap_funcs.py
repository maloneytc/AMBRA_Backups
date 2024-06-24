import pandas as pd
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
from redcap import Project
import configparser
import sys



def get_redcap_project(proj_name, config_path=None):
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = Path.home().joinpath('.redcap.cfg')
    if not config_file.exists():
        logging.error(f'Could not find credentials file: {config_file}')

    config = configparser.ConfigParser()
    config.read(config_file)
    proj_config = config[proj_name]
    return Project('https://redcap.research.cchmc.org/api/', proj_config['token'])



def details_to_dict(log_details):
    """
    handles details from log['details'] and returns a dictionary.
    commas exist in comment field so cannot simply split(',')
    """
    questions = {}
    last_ques = None
    for i, ques in enumerate(log_details.split(',')):
        if '=' not in ques:
            last_ques += ques
            ques = last_ques
        else:
            if not last_ques:
                key, value = ques.split('=')[0].strip(), ques.split('=')[1].strip()    
            else:
                key, value = last_ques.split('=')[0].strip(), last_ques.split('=')[1].strip()
            value = '1' if value == 'checked' else value
            value = '0' if value == 'unchecked' else value
            questions[key] = value.replace('\'', '')
        last_ques = ques
    return questions



def project_data_to_db(db, project):
    """
    Exports data from redcap logs into db
    1. extract logs from redcap from last successful update to now
    2. insert new patients into db if any new patients
    3. extract then remove instance, complete, and record_id from logs
    4. find crf_name from log questions
    5. if log variables cannot match a crf_name, add to failed_to_add list, 
       otherwise continue to handle crf
    6. if crf_row for (patient,crf_name,instance) does not exist, insert new crf_row
       if exists, update verified/complete if exists and differs from log
    7. insert data into crf_data_redcap

    Note: if a log appears in redcap, but not through the api, this is normal, the api 
          just takes a few minutes
    """


    # internval for logs to be extracted 
    interval_start = db.run_select_query("""SELECT * FROM backup_info_RedCap""")
    if len(interval_start) == 0: # if new project without any backup info, start from 2000, all data
        interval_start = datetime(2000, 1, 1)
    else:
        interval_start = interval_start[0][1]
    interval_end = datetime.now()
    logs = project.export_logging(begin_time=interval_start, end_time=interval_end)


    # getting logs that modify records
    record_action = ['Update record', 'Create record']
    record_logs = [log for log in logs if re.sub(r'\d+$', '', log['action']).strip() in record_action]
    record_logs.reverse() # list starts with most recent. Flipping order to update chronologically


    # dictionary of form names and their variables
    master_form_var_dict = {}
    for var in project.metadata:
        if var['field_name'] == 'record_id': continue # not necessary. record_id will be created at creation of a patient in redcap
        if var['form_name'] not in master_form_var_dict:
            master_form_var_dict[var['form_name']] = [var['field_name']]
        else:
            master_form_var_dict[var['form_name']].append(var['field_name'])


    # list of forms that have repeating instruments
    form_names = [form['instrument_name'] for form in project.export_instruments()]
    repeating_forms = []
    if project.export_project_info()['has_repeating_instruments_or_events'] == 1: 
        rep_forms = [form['form_name'] for form in project.export_repeating_instruments_events()]
        for name in form_names: 
            if name in rep_forms:
                repeating_forms.append(name)



    # list of current patients in db to check if there is a new patient
    patient_names = [p[0] for p in db.run_select_query("""SELECT patient_name FROM patients""")]

    
    # loop through record_logs and add to db
    failed_to_add = []
    for i, log in tqdm(enumerate(record_logs), total=len(record_logs), desc='Adding data logs to db'):
        if log['details'] == '': continue # no changes to record
        
        
        # compare forms, if any error differences, raise error
        # if log['action'] == 'Manage/Design ':
        #     comp_redcap_and_db_schemas(db, project)



        patient_name = log['action'].split(' ')[-1].strip()
        if patient_name not in patient_names:
            db.run_insert_query(f"""INSERT INTO patients (patient_name, patient_id) VALUES (%s, %s)""", [patient_name, patient_name])
        else:
            patient_id = str(db.run_select_query(f"""SELECT id FROM patients WHERE patient_name = %s""", [patient_name])[0][0])


        # formatting detail string into dictionary of ques: value
        ques_value_dict = details_to_dict(log['details'])


        # removing following fields from ques:value to insert to db:
        # instance
        instance = None
        if '[instance' in ques_value_dict.keys():
            instance = int(ques_value_dict['[instance'][0])
            ques_value_dict.pop('[instance')
        # complete fields
        all_form_complete_fields = set(form+'_complete' for form in master_form_var_dict.keys())
        complete_field_intersection = all_form_complete_fields.intersection(set(ques_value_dict.keys()))
        if complete_field_intersection:
            n = complete_field_intersection.pop()
            ques_value_dict.pop(n)
        # record_id
        if 'record_id' in ques_value_dict.keys():
            ques_value_dict.pop('record_id')

        # if after removing instance, complete, and record_id, ques:value is empty, nothing to add to database 
        if len(ques_value_dict) == 0:
            continue


        # The intersection of the log variables and the master form variables give the sub set 
        # belonging to the form of interest
        log_ques = set([ques_value.split('(')[0].strip() for ques_value in ques_value_dict.keys()])
        crf_name = None
        for form, form_ques in master_form_var_dict.items():
            if log_ques.intersection(set(form_ques)):
                crf_name = form

        # if no form found, log variable is not up to date with current redcap variables
        if crf_name is None:
            failed_to_add.append((patient_name, log['timestamp'], ques_value_dict))
            # raise ValueError(f'No matching form found for the following questions: {log_ques}')


        # the instance variable will not appear in the log if it is the first instance of a repeating form
        if (instance is None) and (crf_name in repeating_forms):
            instance = 1


        # try to grab crf_row from patient, if none insert new crf_row
        # if exists, check if verified needs updated
        crf_row = db.run_select_query(f"""SELECT * FROM CRF_RedCap WHERE id_patient = {patient_id} AND crf_name = \'{crf_name}\' 
                                      AND instance {'IS NULL' if instance is None else f'= {instance}'}""") # cant use record here, because ('IS NULL' or '= #') is not a sql variable
        if len(crf_row) == 0:
            if f'{crf_name}_status' in ques_value_dict.keys():
                if ques_value_dict[f'{crf_name}_status'] == '4':
                    verified = 1
                else:
                    verified = 0
                ques_value_dict.pop(f'{crf_name}_status')
            else:
                verified = 0 # new form, not in log, verified @DEFAULT = '0'
            crf_id = db.run_insert_query(f"""INSERT INTO CRF_RedCap (id_patient, crf_name, instance, verified) VALUES 
                                         ({patient_id}, \'{crf_name}\', {'NULL' if instance is None else instance}, {verified})""", None) 
        # if crf does exist, grab crf_id, and check if db_verified needs updated
        else:
            crf_id = crf_row[0][0]
            db_verified = crf_row[0][-1]
            if f'{crf_name}_status' in ques_value_dict.keys():
                if db_verified != ques_value_dict[f'{crf_name}_status']:
                    if ques_value_dict[f'{crf_name}_status'] == '4':
                        verified = 1
                    else:
                        verified = 0
                    db.run_insert_query(f"UPDATE CRF_RedCap SET verified = %s WHERE id = %s", [verified, crf_row[0][0]])
                ques_value_dict.pop(f'{crf_name}_status')
            else:
                verified = db_verified # for print out 


        # insert data into crf_data_redcap
        for key, value in ques_value_dict.items():
            db.run_insert_query("""
            INSERT INTO CRF_Data_RedCap (id_crf, value, redcap_variable)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE value=%s;
            """, (crf_id, value, key,
                    value))           
            

    return failed_to_add


# using main for testing purposes, manual backups
if __name__ == '__main__':


    import AMBRA_Backups
    import AMBRA_Utils


    testing = 0
    if testing:
        project = get_redcap_project('14102 Khandwala-Radiology Imaging Services Core Lab Workflow')
        db = AMBRA_Backups.database.Database('CAPTIVA_Test')
    else:
        project = get_redcap_project('CAPTIVA Data Collection')
        db = AMBRA_Backups.database.Database('CAPTIVA')

    # manual backup
    # start_date = datetime(2023, 1, 1)
    # db.run_insert_query("""UPDATE backup_info_RedCap SET last_backup = %s""", [start_date])
    failed_to_add = project_data_to_db(db, project)
    # saving logs that failed to upload to database
    if failed_to_add:
        file_path = Path('/Volumes/CAPTIVA/redcap_backup_info/log_of_failed_redcap_logs.txt')
        op = 'a' if file_path.exists() else 'w'
        with open(str(file_path), op) as file:
            file.write('------------------------------------\n')
            file.write(f'data export date: {datetime.now().strftime("%Y-%m-%d")}\n')
            for log in failed_to_add:
                file.write('------------\n')
                file.write(f'Patient_name: {log[0]}\n')
                file.write(f'Date of redcap entry: {log[1]}\n')
                file.write(f'Question-values:\n')
                for var in log[2]:
                    file.write(f'{var} : {log[2][var]}\n')
            
    db.run_insert_query("""UPDATE backup_info_RedCap SET last_backup = %s""", [datetime.now()])


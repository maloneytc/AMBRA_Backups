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
import json

from AMBRA_Backups import utils


def get_config(config_path=None):
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = Path.home().joinpath('.redcap.cfg')
    if not config_file.exists():
        logging.error(f'Could not find credentials file: {config_file}')

    config = configparser.ConfigParser()
    config.read(config_file)

    return config


def get_redcap_project(proj_name, config_path=None):
    config = get_config(config_path=config_path)    
    proj_config = config[proj_name]
    return Project('https://redcap.research.cchmc.org/api/', proj_config['token'])


def backup_project(project_name, url, api_key, output_dir):
    """
    """
    project = Project(url, api_key)
    
    # Info
    # ---------------
    info_out = output_dir.joinpath(f'{project_name}_info.json')
    info_json = {"Project Name": project_name,
                 "RedCap version": str(project.export_version()),
                 "Backup date": datetime.now().strftime("%m/%d/%Y %H:%M:%S")}
    with open(info_out, 'w', encoding='utf-8') as f:
        json.dump(info_json, f, ensure_ascii=False, indent=4)
    
    # Data Dictionary
    # ---------------
    meta_json = project.export_metadata(format_type='json')
    meta_out = output_dir.joinpath(f'{project_name}_metadata.json')
    with open(meta_out, 'w', encoding='utf-8') as f:
        json.dump(meta_json, f, ensure_ascii=False, indent=4)

    # Data 
    # ---------------
    forms_json = project.export_records(format_type='json')
    forms_out = output_dir.joinpath(f'{project_name}_forms.json')
    with open(forms_out, 'w', encoding='utf-8') as f:
        json.dump(forms_json, f, ensure_ascii=False, indent=4)

    # Users
    # ---------------
    users_json = project.export_users(format_type='json')
    users_out = output_dir.joinpath(f'{project_name}_users.json')
    with open(users_out, 'w', encoding='utf-8') as f:
        json.dump(users_json, f, ensure_ascii=False, indent=4)

    # User roles
    # ---------------
    roles_json = project.export_user_roles(format_type='json')
    roles_out = output_dir.joinpath(f'{project_name}_roles.json')
    with open(roles_out, 'w', encoding='utf-8') as f:
        json.dump(roles_json, f, ensure_ascii=False, indent=4)

    role_assignment_json = project.export_user_role_assignment(format_type='json')
    role_assignment_out = output_dir.joinpath(f'{project_name}_roles_assignment.json')
    with open(role_assignment_out, 'w', encoding='utf-8') as f:
        json.dump(role_assignment_json, f, ensure_ascii=False, indent=4)

    # Form-event mappings
    # fem = project.export_fem()

    # Files - Would need to loop over subjects and fields and run
    # project.export_file(record=, field=)

    # Repeating instruments
    # ---------------
    try:
        repeating_json = project.export_repeating_instruments_events(format_type='json')
        repeating_out = output_dir.joinpath(f'{project_name}_repeating.json')
        with open(repeating_out, 'w', encoding='utf-8') as f:
            json.dump(repeating_json, f, ensure_ascii=False, indent=4)
    except:
        pass


def details_to_dict(log_details):
    """
    Converts log details string into dictionary of questions and values
    Splits on '=' because ',' can be used in comment fields, so not reliable to split on
    """
    
    
    # repalcements
    log_details = log_details.replace('unchecked', '0').replace('checked', '1')
    
    questions = {}
    strings = log_details.split('=')

    if len(strings) == 2:
        questions[strings[0].strip()] = strings[1].strip().strip('\'')
        return questions

    for i in range(0, len(strings)-1):
        if i == 0:
            questions[strings[i].strip()] = ','.join(strings[i+1].split(',')[:-1]).strip()
        elif i == len(strings)-2:
            questions[strings[i].split(',')[-1].strip()] = strings[i+1].strip()
        else:
            questions[strings[i].split(',')[-1].strip()] = ','.join(strings[i+1].split(',')[:-1]).strip()

    # removing extra 's from questions without removing purposeful 's
    for question in questions:
        questions[question] = questions[question].strip('\'')
    
    return questions




def grab_logs(db, project, only_record_logs, start_date=None, end_date=None):
    """
    Extracts logs from redcap from start_date to end_date
    If only_record_logs is true only logs that modify records are extracted
    """
    if start_date is None:
        start_date = db.run_select_query("""SELECT * FROM backup_info_RedCap""")
        if len(start_date) == 0: # if new project without any backup info, start from 2000, all data
            start_date = datetime(2000, 1, 1)
        else:
            start_date = start_date[0][1]
    if end_date is None:
        end_date = datetime.now()
    logs = project.export_logging(begin_time=start_date, end_time=end_date)

    if only_record_logs:
        # getting logs that modify records
        record_action = ['Update record', 'Create record', 'Delete record']
        record_logs = [log for log in logs if re.sub(r'\d+$', '', log['action']).strip() in record_action]
        record_logs.reverse() # list starts with most recent. Flipping order to update chronologically
        logs = record_logs

    return logs


def get_form_df(project, patient_name, crf_name, instance):
    """
    project.export_records() has a bug where if the first form of a project is a repeating form
    an extra row of data is added with residual when exporting from subsequent forms.
    This function removes the residual row if it exists

    8/7/24 bug: If trying to get a empty repeating form, then doing export_record error handling, 
                will be left with an empty dataframe which is lastly checked if all values are '', which
                results in an error.
                Workaround: call project_data_to_db again as crf will exist in CRF_RedCap from previous call,
                which will prompt the logs to be used for data insertion instead of the api call from this function
    """
    form_df = pd.DataFrame(project.export_records(records=[patient_name], forms=[crf_name]))
    if form_df.empty:
        return pd.DataFrame({})
    if project.export_project_info()['has_repeating_instruments_or_events']:
        repeating_forms = [f['form_name'] for f in project.export_repeating_instruments_events()]
        if crf_name in repeating_forms:
            form_df = form_df.loc[(form_df['redcap_repeat_instrument'] == crf_name) & (form_df['redcap_repeat_instance'] == instance)]
        else:
            form_df = form_df.iloc[0].to_frame().T
    else:
        form_df = form_df.iloc[0].to_frame().T

    if all(value == '' for value in form_df[form_df.columns[1:]].iloc[0]): 
        return pd.DataFrame({})
    return form_df


def project_data_to_db(db, project, start_date=None, end_date=None):
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
    8. if any logs failed to add, raise error with failed_to_add list
    9. update last export time in backup_info_RedCap

    Note: if a log appears in redcap, but not through the api, this is normal, the api 
          just takes a few minutes
    """


    # try: 

    # internval for logs to be extracted 
    only_record_logs = True
    record_logs = grab_logs(db, project, only_record_logs, start_date, end_date)

    # dictionary of form names and their variables
    master_form_var_dict = {}
    for var in project.metadata:
        if var['field_name'] == 'record_id': continue # not necessary. record_id will be created at creation of a patient in redcap
        if var['form_name'] not in master_form_var_dict:
            master_form_var_dict[var['form_name']] = [var['field_name']]
        else:
            master_form_var_dict[var['form_name']].append(var['field_name'])
    for form in [f['instrument_name'] for f in project.export_instruments()]:
        master_form_var_dict[form].append(f'{form}_complete')


    # list of forms that have repeating instruments
    form_names = [form['instrument_name'] for form in project.export_instruments()]
    repeating_forms = []
    if project.export_project_info()['has_repeating_instruments_or_events'] == 1: 
        rep_forms = [form['form_name'] for form in project.export_repeating_instruments_events()]
        for name in form_names: 
            if name in rep_forms:
                repeating_forms.append(name)
    
    
    # loop through record_logs and add to db
    failed_to_add = []
    for i, log in tqdm(enumerate(record_logs), total=len(record_logs), desc='Adding data logs to db'):
        if log['details'] == '': continue # no changes to record
        
        
        # compare forms, if any error differences, raise error
        # if log['action'] == 'Manage/Design ':
        #     comp_redcap_and_db_schemas(db, project)


        # log deleting a record
        if re.sub(r'\d+$', '', log['action']).strip() == 'Delete record':
            patient_name = log['action'].split(' ')[-1].strip()
            patient_id = str(db.run_select_query(f"""SELECT id FROM patients WHERE patient_name = %s""", [patient_name])[0][0])
            db.run_insert_query(f"""UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s""", [patient_id])
            continue


        # formatting detail string into dictionary of ques: value
        ques_value_dict = details_to_dict(log['details'])


        # removing following fields from ques:value to insert to db:
        # instance
        instance = None
        if '[instance' in ques_value_dict.keys():
            instance = int(ques_value_dict['[instance'][0])
            ques_value_dict.pop('[instance')
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


        # list of current patients in db to check if there is a new patient
        patient_names = [p[0] for p in db.run_select_query("""SELECT patient_name FROM patients""")]
        patient_name = log['action'].split(' ')[-1].strip()
        if patient_name not in patient_names:
            db.run_insert_query(f"""INSERT INTO patients (patient_name, patient_id) VALUES (%s, %s)""", [patient_name, patient_name])
        patient_id = str(db.run_select_query(f"""SELECT id FROM patients WHERE patient_name = %s""", [patient_name])[0][0])


        # if no form found, log variable is not up to date with current redcap variables
        if crf_name is None:
            failed_to_add.append((patient_name, log['timestamp'], ques_value_dict))
            continue
            # raise ValueError(f'No matching form found for the following questions: {log_ques}')
        

        # the instance variable will not appear in the log if it is the first instance of a repeating form
        if (instance is None) and (crf_name in repeating_forms):
            instance = 1


        # try to grab crf_row from patient, if none insert new crf_row
        # if exists, check if verified needs updated
        crf_row = db.run_select_query(f"""SELECT * FROM CRF_RedCap WHERE id_patient = {patient_id} AND crf_name = \'{crf_name}\' 
                                    AND instance {'IS NULL' if instance is None else f'= {instance}'} AND deleted = '0'""") # cant use record here, because ('IS NULL' or '= #') is not a sql variable
        if len(crf_row) == 0:

            # inserting crf
            if f'{crf_name}_status' in ques_value_dict.keys():
                if ques_value_dict[f'{crf_name}_status'] == '4' or ques_value_dict[f'{crf_name}_status'] == '5':
                    verified = 1
                else:
                    verified = 0
                # dont need to pop it off here since it is only used to extract verified, not for data insertion
            else:
                verified = 0
            deleted = 0
            crf_id = db.run_insert_query(f"""INSERT INTO CRF_RedCap (id_patient, crf_name, instance, verified, deleted) VALUES 
                                            (%s, %s, {'NULL' if instance is None else instance}, %s, %s)""", [patient_id, crf_name, verified, deleted])
            
            
            form_df = get_form_df(project, patient_name, crf_name, instance)
            if form_df.empty: # if empty, means there is no live data for this patient and a deleted log should appear later
                continue 
            irr_cols = 3 if form_df.columns[1] == 'redcap_repeat_instrument' else 1 # number of irrelevant fields ie. record_id, redcap_repeat_instrument, redcap_repeat_instance
            form_df = form_df[form_df.columns[irr_cols:]]
            form_df = form_df.melt(var_name='redcap_variable')
            form_df.loc[form_df['redcap_variable'].str.contains('___'), 'redcap_variable'] = form_df['redcap_variable']+')'
            form_df.loc[form_df['redcap_variable'].str.contains('___'), 'redcap_variable'] = form_df['redcap_variable'].str.replace('___', '(')
            form_df['id_crf'] = crf_id

            # inserting data
            utils.df_to_db_table(db, form_df, 'CRF_Data_RedCap')


        # if crf does exist, grab crf_id, and check if db_verified needs updated
        else:
            crf_id = crf_row[0][0]
            if f'{crf_name}_status' in ques_value_dict.keys():
                if ques_value_dict[f'{crf_name}_status'] == '4' or ques_value_dict[f'{crf_name}_status'] == '5':
                    verified = 1
                else:
                    verified = 0
                db.run_insert_query(f"UPDATE CRF_RedCap SET verified = %s WHERE id = %s", [verified, crf_row[0][0]])
                ques_value_dict.pop(f'{crf_name}_status')


            # insert data into crf_data_redcap
            for key, value in ques_value_dict.items():
                db.run_insert_query("""
                INSERT INTO CRF_Data_RedCap (id_crf, value, redcap_variable)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE value=%s;
                """, (crf_id, value, key,
                        value))           
        


    # After trying to add all the logs, if there are any logs with questions not attached
    # to a current crf (outdated variable), they will be printed to an error string 
    if failed_to_add:
        failed_string = ''
        failed_string = failed_string + '------------------------------------\n'
        failed_string = failed_string + f'data export date: {datetime.now().strftime("%Y-%m-%d")}\n'
        for log in failed_to_add:
            failed_string = failed_string + '------------\n'
            failed_string = failed_string + f'Patient_name: {log[0]}\n'
            failed_string = failed_string + f'Date of redcap entry: {log[1]}\n'
            failed_string = failed_string + f'Question-values:\n'
            for var in log[2]:
                failed_string = failed_string + f'{var} : {log[2][var]}\n'


        # this class is a solution I found to get the failed_string to print newline characters 
        class KeyErrorMessage(str): 
            def __repr__(self): return str(self)
        msg = KeyErrorMessage(failed_string)
        raise KeyError(msg)   

    # except Exception as e:
    #     print(f"Error backing up RedCap data: {e}")


    # if export successful, update the last export time
    project_name = project.export_project_info()['project_title']
    db.run_insert_query('UPDATE backup_info_RedCap SET last_backup = %s WHERE project_name = %s', [datetime.now(), project_name])





# using main for testing purposes, manual backups
if __name__ == '__main__':


    import AMBRA_Backups
    import AMBRA_Utils


    testing = 0
    db_name = 'TESTED'
    project_name = 'TESTED Data Collection'
    # db_name = 'SISTER'
    # project_name = '29423 Vagal - SISTER'
    if testing:
        db = AMBRA_Backups.database.Database('CAPTIVA_Test')
        project = get_redcap_project('14102 Khandwala-Radiology Imaging Services Core Lab Workflow')
    else:
        db = AMBRA_Backups.database.Database(db_name)
        project = get_redcap_project(project_name)


    # manual backup
    # start_date = datetime(2023, 1, 1)
    # db.run_insert_query("""UPDATE backup_info_RedCap SET last_backup = %s""", [start_date])
    start_date = datetime(2020, 7, 9, 11, 30)
    # end_date = datetime(2024, 7, 1, 13, 41)
    project_data_to_db(db, project, start_date)


    # inserting logs only for select patient
    # project = AMBRA_Backups.redcap_funcs.get_redcap_project('CAPTIVA Data Collection')
    # logs = AMBRA_Backups.redcap_funcs.grab_logs(db, project, 1, start_date)
    # dates = []
    # for log in logs:
    #     if log['record'] == '1006':
    #         dates.append((datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M')+ timedelta(minutes=1), datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M') - timedelta(minutes=1)))

    # for date in dates:
    #     project_data_to_db(db, project, date[1], date[0])



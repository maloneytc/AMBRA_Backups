import pandas as pd
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging
from bs4 import BeautifulSoup

from redcap import Project

import configparser
import sys
config = configparser.ConfigParser()
config.read(Path.home().joinpath('.ambra_loc'))
sys.path.insert(0, config['AMBRA_Backups']['Path'])
sys.path.insert(0, config['AMBRA_Utils']['Path'])
import AMBRA_Backups
import AMBRA_Utils


def get_redcap_project(proj_name, config_path=None):
        if config_path:
            config_file = Path(config_path)
        else:
            config_file = Path.home().joinpath('.redcap_api')
        if not config_file.exists():
            logging.error(f'Could not find the credentials file: {config_file}')

        config = configparser.ConfigParser()
        config.read(config_file)
        proj_config = config[proj_name]
        return Project('https://redcap.research.cchmc.org/api/', proj_config['token'])



def get_record_dict(project):
    return {record['record_id'] : record for record in project.export_records()}



def get_form_var_dict(project):
    """Keys: form names, values: form questions"""
    master_form_var_dict = {}
    for var in project.metadata:
        if var['field_name'] == 'record_id': continue # not necessary. record_id will be created at creation of a patient in redcap
        if var['form_name'] not in master_form_var_dict:
            master_form_var_dict[var['form_name']] = [var['field_name']]
        else:
            master_form_var_dict[var['form_name']].append(var['field_name'])
        return master_form_var_dict



def get_record_logs(project, start, end):
    """
    project - redcap project instance
    start - start date from when to pull logs
    end - end date from when to stop pulling logs
    Returns logs that create/upate records at specific interval.
    In chronological order
    """
    logs = project.export_logging(begin_time=start, end_time=end)
    record_action = ['Update record', 'Create record']
    record_logs = [log for log in logs if re.sub(r'\d+$', '', log['action']).strip() in record_action]
    return record_logs.reverse() # list starts with most recent. Flipping order to update chronologically



def redcap_form_count(project):
    """
    Counts the number of forms with any data filled out in redcap
    """

    # getting variable and record dictionaries
    master_form_var_dict = get_form_var_dict(project)
    record_dict = get_record_dict(project)


    # get the empty record from project for comparision.
    # Will error out if no record 'empty_record'
    if 'empty_record' in record_dict.keys():
        empty_data_dict = record_dict['empty_record']
    else:
        raise ValueError(f"""There is no 'empty_record' in project {project.export_project_info()['project_title']}""")


    # empty record value dictionary
    form_names = [form['instrument_name'] for form in project.export_instruments()]
    form_empty_value_dict = {}
    for form_name in form_names:
        key_subset = master_form_var_dict[form_name]
        form_empty_dict = {key: empty_data_dict[key] for key in key_subset if key in empty_data_dict}
        form_empty_value_dict[form_name] = form_empty_dict


    # counting and returning non-empty forms
    non_empty_count = 0
    for rec in project.export_records():
        for form_name in form_names:
            values_in_form = {key: rec[key] for key in master_form_var_dict[form_name] if key in rec.keys()}
            if form_empty_value_dict[form_name] != values_in_form:
                non_empty_count+=1
    return non_empty_count



def backup_from(db, project, start):
    """
    db - AMBRA_Backups.database.Database instance
    project - redcap project instance
    start - start date from when to pull logs to current date
    Manually backs up db by setting the last successful backup time in backup_info_RedCap, then running export
    """

    db.run_insert_query(f"""
    UPDATE backup_info_RedCap SET last_backup = '{start}' WHERE (`project_name` = '{project.export_project_info()['project_title']}');
    """, None)
    export_crfs_and_crf_data_to_db(db, project)
    db.run_insert_query(f"""
    UPDATE backup_info_RedCap SET last_backup = \'{datetime.today()}\' WHERE (`project_name` = \'{project.export_project_info()['project_title']}\');
    """, None)



def details_to_dict(log_details):
    """
    log_details - string of details from redcap log
    handles details from log['details'] and returns a dictionary
    would simply split(','), but commas exist in comment fields
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



def export_crfs_and_crf_data_to_db(db, project):
    """
    Exports data from redcap logs into db.
    Uses backup_info_RedCap to determine a time interval to look for logs
    """


    # internval for logs to be extracted 
    # interval_start = datetime.strptime(
    #     db.run_select_query("""SELECT * FROM backup_info_RedCap""")[0][1], '%Y-%m-%d %H:%M:%S.%f')
    interval_start = db.run_select_query("""SELECT * FROM backup_info_RedCap""")[0][1]
    interval_end = datetime.now()
    logs = project.export_logging(begin_time=interval_start, end_time=interval_end)


    # getting logs that modify records
    record_action = ['Update record', 'Create record']
    manage_details = ['Delete project field', 'Edit project field', 'Create project field']
    record_logs = [log for log in logs if (re.sub(r'\d+$', '', log['action']).strip() in record_action) or
                                          (log['action'] == 'Manage/Design ' and log['details'] in manage_details)]
    record_logs.reverse() # list starts with most recent. Flipping order to update chronologically


    # map forms to their variables
    master_form_var_dict = {}
    for var in project.metadata:
        if var['field_name'] == 'record_id': continue # not necessary. record_id will be created at creation of a patient in redcap
        if var['form_name'] not in master_form_var_dict:
            master_form_var_dict[var['form_name']] = [var['field_name']]
        else:
            master_form_var_dict[var['form_name']].append(var['field_name'])


    # list of forms that have repeating instruments
    form_names = [form['instrument_name'] for form in project.export_instruments()]
    rep_inst = []
    if project.export_project_info()['has_repeating_instruments_or_events'] == 1: 
        rep_forms = [form['form_name'] for form in project.export_repeating_instruments_events()]
        for name in form_names: 
            if name in rep_forms:
                rep_inst.append(name)


    # handle redcap crf and crf_data for each log
    failed_to_add = []
    for i, log in enumerate(record_logs):
        if log['details'] == '': continue # no changes to record
        
        
        # compare forms, if any error differences, raise error
        if log['action'] == 'Manage/Design ':
            comp_redcap_and_db_schemas(db, project)


        # patient id from record_id. updated fields from log['details']
        patient_id = int(log['action'].split(' ')[-1].strip())
        ques_value_dict = details_to_dict(log['details'])


        # if repeating instrument, pull out instance number
        instance = None
        if '[instance' in ques_value_dict.keys():
            instance = int(ques_value_dict['[instance'][0])
            ques_value_dict.pop('[instance')


        # removing complete field from data to populate
        all_form_complete_fields = set(form+'_complete' for form in master_form_var_dict.keys())
        complete_field_intersection = all_form_complete_fields.intersection(set(ques_value_dict.keys()))
        if complete_field_intersection:
            n = complete_field_intersection.pop()
            ques_value_dict.pop(n)


        # record_id is the patient_id, not to be put into data table
        if 'record_id' in ques_value_dict.keys():
            ques_value_dict.pop('record_id')


        # if <form_name>_complete and record_id were the only changes, nothing to add to database
        if len(ques_value_dict) == 0:
            continue


        # find form name from log variables
        log_ques = set([ques_value.split('(')[0].strip() for ques_value in ques_value_dict.keys()])
        crf_name = None
        for form, all_ques in master_form_var_dict.items():
            if log_ques.intersection(set(all_ques)):
                crf_name = form


        # if the first instance of a repeated form, instance variable will not appear
        if (instance is None) and crf_name in rep_inst:
            instance = 1


        # if no form found, log variables are not up to date with current redcap variables
        if crf_name is None:
            failed_to_add.append(log_ques)
            raise ValueError(f'No matching form found for the following questions: {log_ques}')


        # grab crf_row. If no crf, check for verified data point to 
        # determine verified, then create new crf_row with new crf_id
        crf_row = db.run_select_query(f"""SELECT * FROM CRF_RedCap WHERE id_patient = {patient_id} AND crf_name = \'{crf_name}\' 
                                      AND instance {'IS NULL' if instance is None else f'= {instance}'}""")
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
                    db.run_insert_query(f"UPDATE CRF_RedCap SET verified = \'{verified}\' WHERE id = {crf_row[0][0]}", None)
                ques_value_dict.pop(f'{crf_name}_status')
            else:
                verified = db_verified


        # insert data into crf_data_redcap
        for key, value in ques_value_dict.items():
            db.run_insert_query("""
            INSERT INTO CRF_Data_RedCap (id_crf, value, redcap_variable)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE value=%s;
            """, (crf_id, value, key,
                    value))           


        print(f'form: {crf_name}')
        print(f'fields: {ques_value_dict}')
        print(f'instance: {instance}')
        print(f'verified: {verified}')
        print('-----')



def comp_redcap_and_db_schemas(db, project):

    """
    cycles through forms in project and compares redcap schema to database schema
    called in export_crfs_and_crf_data_to_db
    if any differences found, add to error log which is printed out
    at the end of the difference comparision cycle

    db - AMBRA_Backups.database.Database instance
    project - redcap project instance
    """


    # differences across schemas
    diffs = {form: [] for form in project.forms}
    for form in project.forms:
        form_diffs = []


        # grab database schema, if none exists, return none
        db_df = pd.DataFrame(db.run_select_query(f"""SELECT * FROM CRF_Schema_RedCap 
                                        WHERE crf_name = '{form}'""", column_names=True))
        db_df = db_df.fillna('NULL')
        db_df = db_df.replace("'", "\\'", regex=True)
        if db_df.empty:
            form_diffs.append(f'No schema for form: {form}')
            continue
        

        # grab redcap schema
        schema_df = db_formated_schema(project, form)


        # same columns
        db_df = db_df[schema_df.columns]


        # if new is bigger than old, questions were deleted, add error to diff
        if set(db_df['redcap_variable']) > set(schema_df['redcap_variable']):
            for redcap_var in set(db_df['redcap_variable']) - set(schema_df['redcap_variable']):
                form_diffs.append(f'ERROR: RedCap variable in db: {redcap_var} was not found redcap version. Deleted?')
            db_df = db_df[db_df['redcap_variable'].isin(schema_df['redcap_variable'])]


        # if old is bigger than new, questions were added, add notice to diff
        else:
            for redcap_var in set(schema_df['redcap_variable']) - set(db_df['redcap_variable']):
                form_diffs.append(f'New redcap_variable: {redcap_var} in redcap')
            schema_df = schema_df[schema_df['redcap_variable'].isin(db_df['redcap_variable'])]


        # check for question changes
        for redcap_var in db_df['redcap_variable']:
            for col in set(db_df.columns)-{'redcap_variable'}:
                old_val = db_df.loc[db_df['redcap_variable'] == redcap_var, col].iloc[0]
                new_val = schema_df.loc[schema_df['redcap_variable'] == redcap_var, col].iloc[0]
                if old_val != new_val:
                    form_diffs.append(f'At ({redcap_var}, {col}) db: {old_val}, redcap: {new_val}')


        # field depreciation
        """Does not seem necessary at this point. Can capture in question changes with field_annotation"""
        # for redcap_var in db_df['redcap_variable']:
        #     old_val = db_df.loc[db_df['redcap_variable'] == redcap_var, 'field_annotation'].iloc[0]
        #     new_val = schema_df.loc[schema_df['redcap_variable'] == redcap_var, 'field_annotation'].iloc[0]
        #     if '@HIDDEN' in old_val or '@HIDEEN' in new_val:
        #         if old_val != new_val:
        #             form_diffs.append(f'''Depreciation status change for 
        #                         redcap variable {redcap_var}: Orignal: {old_val}, new: {new_val}''')
                

        # end
        diffs[form] = form_diffs


    # cycling through and printing form differences
    any_errors = False
    for key in diffs.keys():
        if diffs[key]:
            any_errors = True
            print('-----')
            print(f'Form: {key} \n')
            for value in diffs[key]:
                print(f'{value}')
            print('-----')


    if any_errors:
        raise ValueError(f"""Correct for ERROR differences between RedCap and database forms""")
    else:
        print('---- \nNo differences between RedCap and database forms\n ----')


    

def db_formated_schema(project, form):


    # check form belongs to project. Also checks for csv extension
    forms_with_csvs = project.forms + [form + '.csv' for form in project.forms]
    if form not in forms_with_csvs:
        raise ValueError(f"""Form {form} does not belong to project 
                         {project.export_project_info()['project_title']} with forms: {project.forms}""")


    # if passing in a csv, dont use api, load file
    if '.csv' in form:
        df = pd.read_csv(form)
        df = df[['Form Name', 'Variable / Field Name', 'Field Label', 'Choices, Calculations, OR Slider Labels',
                  'Text Validation Type OR Show Slider Number', 'Field Note', 'Field Type']]
    else:
        df = pd.DataFrame([var for var in project.metadata if var['form_name'] == form])
        df = df[['form_name', 'field_name', 'field_label', 'select_choices_or_calculations', 
                 'text_validation_type_or_show_slider_number', 'field_note', 'field_type', 'field_annotation']]
        # changing api column names to csv column names
        df.columns = [['Form Name', 'Variable / Field Name', 'Field Label', 'Choices, Calculations, OR Slider Labels', 
                       'Text Validation Type OR Show Slider Number', 'Field Note', 'Field Type', 'Field Annotation']]
        # record_id not stored in data
        df = df[df['Variable / Field Name'] != 'record_id']


    
    schema_df = pd.DataFrame(columns=df.columns)
    for i, row in df.iterrows():
        new_row = row
        new_row.index = df.columns


        # change date and time to string
        if not pd.isna(new_row['Text Validation Type OR Show Slider Number']):
            if new_row['Text Validation Type OR Show Slider Number'] in ['date_mdy', 'time']:
                new_row['Text Validation Type OR Show Slider Number'] = 'string'


        # if field type is checkbox, create new rows for each option
        if new_row['Field Type'] == 'checkbox':
            values = [option.split(',')[1].strip() for option in 
                    new_row['Choices, Calculations, OR Slider Labels'].split('|')]
            options = [option.split(',')[0].strip() for option in 
                    new_row['Choices, Calculations, OR Slider Labels'].split('|')]
            question = new_row['Variable / Field Name']
            new_row['Text Validation Type OR Show Slider Number'] = 'int'
            for option, value in zip(options, values):
                # new_row['Choices, Calculations, OR Slider Labels'] = f"{question}({option})"
                new_row['Choices, Calculations, OR Slider Labels'] = value
                new_row['Variable / Field Name'] = f"{question}({option})"
                # new_row['Field Label'] = value
                schema_df = pd.concat([schema_df, new_row.to_frame().T], ignore_index=True)


        # radio button
        elif new_row['Field Type'] == 'radio':
            data_labels = []
            for option in new_row['Choices, Calculations, OR Slider Labels'].split('|'):
                if option.count(',') > 1:
                    data_labels.append(f"{option.split(',')[0].strip()}={','.join(option.split(',')[1:])}")
                else:
                    data_labels.append(f"{option.split(',')[0].strip()}={option.split(',')[1].strip()}")
            data_labels = " | ".join(data_labels)
            new_row['Choices, Calculations, OR Slider Labels'] = data_labels
            new_row['Text Validation Type OR Show Slider Number'] = 'int'
            schema_df = pd.concat([schema_df, new_row.to_frame().T], ignore_index=True)


        # embedded tables handled after all other fields
        elif new_row['Field Type'] == 'descriptive':
            # continue
            schema_df = pd.concat([schema_df, new_row.to_frame().T], ignore_index=True)


        # calculated field
        elif new_row['Field Type'] == 'calc':
            new_row['Text Validation Type OR Show Slider Number'] = 'float'
            new_row['Choices, Calculations, OR Slider Labels'] = 'NULL'
        
        
        # text field
        else:
            new_row['Text Validation Type OR Show Slider Number'] = 'string'
            new_row['Choices, Calculations, OR Slider Labels'] = 'NULL'
            schema_df = pd.concat([schema_df, new_row.to_frame().T], ignore_index=True)


    # version and column names
    schema_df.columns = ['crf_name', 'data_id', 'question_text', 'data_labels', 
                            'data_type', 'field_note', 'question_type', 'field_annotation']
    schema_df['redcap_variable'] = schema_df['data_id']
    schema_df = schema_df[['crf_name', 'data_id', 'redcap_variable', 'question_text', 
                            'data_labels', 'data_type', 'question_type', 'field_note', 'field_annotation']]
    schema_df.fillna('NULL', inplace=True)
    schema_df = schema_df.applymap(lambda x: x.replace("'", "\\'"))


    # embedded table handeling
    if form == 'baseline_brain_crf' or form == 'baseline_brain_crf.csv':
        schema_df = baseline_brain_format(schema_df)
    elif form == 'follow_up_brain_crf' or form == 'follow_up_brain_crf.csv':
        schema_df = follow_up_format(schema_df)
    elif form == 'baseline_vascular_crf' or form == 'baseline_vascular_crf.csv':
        schema_df = baseline_vascular_format(schema_df)


    return schema_df



def baseline_brain_format(schema_df):
    for i, row in schema_df.iterrows():
        # html parsing
        if row['question_type'] == 'descriptive':
            if row['data_id'] == 'q1006_table' or row['data_id'] == 'q3013_table':
                soup = BeautifulSoup(row['question_text'], 'lxml')
                rows = soup.find_all('tr')
                first_row_cells = rows[0].find_all('td')
                ques_text = soup.find('tbody').find_all('tr')[0].find('td').text
                ques_ids = [first_row_cells[1].text.replace("{", "").replace("}", ""), 
                            first_row_cells[2].text.replace("{", "").replace("}", "")]
            elif row['data_id'] == 'q1008_table':
                soup = BeautifulSoup(row['question_text'], 'html.parser')
                ques_text = soup.find('tbody').find_all('tr')[0].find('td').text
                rows = soup.find_all('tr')
                ques_ids = [rows[2].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[2].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[5].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[5].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[7].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[7].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[9].find_all('td')[0].text.replace("{", "").replace("}", "")]
            strings = '|'.join(ques_ids)
            table_mask = schema_df['redcap_variable'].str.contains(strings)
            # schema_df.loc[table_mask, 'data_labels'] = schema_df.loc[table_mask, 'question_text']
            schema_df.loc[schema_df['redcap_variable'].str.contains(strings), 'question_text'] = ques_text
            schema_df = schema_df.drop(i)
        # non embeded check box
        elif 'q1011' in row['data_id']:
            schema_df.loc[schema_df['redcap_variable'].str.contains('q1011'), 'question_text'] = 'Q1011. Acute hemorrhage classification (Check all that are applicable)'
    

    # reset index, question_sub_text, reorder columns
    schema_df = schema_df.reset_index(drop=True)
    schema_df['question_sub_text'] = 'NULL'
    schema_df = schema_df[['crf_name', 'data_id', 'redcap_variable', 'question_text',
                            'question_sub_text', 'data_labels', 'data_type', 'question_type', 'field_note', 'field_annotation']]
    return schema_df



def follow_up_format(schema_df):
    for i, row in schema_df.iterrows():
        # html parsing
        if row['question_type'] == 'descriptive':
            if row['data_id'] == 'q3008_table' or row['data_id'] == 'q3013_table':
                soup = BeautifulSoup(row['question_text'], 'lxml')
                rows = soup.find_all('tr')
                first_row_cells = rows[0].find_all('td')
                q3008_col1 = first_row_cells[1].text.replace("{", "").replace("}", "")
                q3008_col2 = first_row_cells[2].text.replace("{", "").replace("}", "")
                ques_text = soup.find('tbody').find_all('tr')[0].find('td').text
                ques_ids = [q3008_col1, q3008_col2]
            elif row['data_id'] == 'q3011_table':
                soup = BeautifulSoup(row['question_text'], 'html.parser')
                ques_text = soup.find('tbody').find_all('tr')[0].find('td').text
                rows = soup.find_all('tr')
                ques_ids = [rows[2].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[2].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[5].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[5].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[7].find_all('td')[1].text.replace("{", "").replace("}", ""),
                rows[7].find_all('td')[2].text.replace("{", "").replace("}", ""),
                rows[9].find_all('td')[0].text.replace("{", "").replace("}", "")]
            strings = '|'.join(ques_ids)
            schema_df.loc[schema_df['redcap_variable'].str.contains(strings), 'question_text'] = ques_text
            schema_df = schema_df.drop(i)
        # regualar, non embedded check box  
        elif 'q3016' in row['data_id']: 
            schema_df.loc[schema_df['redcap_variable'].str.contains('q3016'), 'question_text'] = 'Q3016. Acute hemorrhage classification (Check all that are applicable)'
    
    
    # reset index, question_sub_text, reorder columns
    schema_df = schema_df.reset_index(drop=True)
    schema_df['question_sub_text'] = 'NULL'
    schema_df = schema_df[['crf_name', 'data_id', 'redcap_variable', 'question_text', 'question_sub_text', 
                        'data_labels', 'data_type', 'question_type', 'field_note', 'field_annotation']]
    return schema_df



def baseline_vascular_format(schema_df):

    # embedded tables to check for
    tables = ['q2005_table', 'q2006_table', 'q2007_table', 'q2008_table', 'q2009_table', 'q2010_table', 
          'q2011_table', 'q2012_table', 'q2013_table', 'q2014_table', 'q2015_table', 'q2016_table', 
          'q2017_table', 'q2018_table', 'q2019_table', 'q2020_table', 'q2021_table', 'q2022_table', 
          'q2023_table', 'q2024_table', 'q2025_table', 'q2026_table', 'q2027_table', 'q2028_table', 
          'q2029_table', 'q2030_table']
    

    # html parsing
    for i, row in schema_df.iterrows():
        if row['redcap_variable'] in tables:
            ques_text = BeautifulSoup(row['question_text']).find('tbody').find_all('tr')[1].find('td').text
            ques_id = row['redcap_variable'].strip('_table')
            table_con = schema_df['redcap_variable'].str.contains(ques_id)
            schema_df.loc[table_con, 'question_sub_text'] = schema_df.loc[table_con, 'question_text']
            schema_df.loc[schema_df['redcap_variable'].str.contains(ques_id), 'question_text'] = ques_text
            schema_df = schema_df.drop(i)

    # reset index, reorder columns
    schema_df = schema_df.reset_index(drop=True)
    schema_df = schema_df[['crf_name', 'data_id', 'redcap_variable', 'question_text',
                            'question_sub_text', 'data_labels', 'data_type', 'question_type', 'field_note', 'field_annotation']]
    return schema_df
    




def df_to_db_table(db, df, table_name):
    """
    inputs df rows into table. Table must exist and have the same columns as df
    """

    # if table not not in db, error
    if table_name not in [table[0] for table in db.run_select_query('SHOW TABLES')]:
        raise ValueError(f'Table {table_name} not in database')
    

    # all df's columns must be in table
    table_columns = [col[0] for col in db.run_select_query(f"SHOW COLUMNS FROM {table_name}") if col[0] != 'id']
    if not set(df.columns) <= set(table_columns):
        raise ValueError(f'''Columns in dataframe not in table {table_name}:
                         \ndf columns: \n{df.columns.to_list()}\ntable columns: \n{table_columns}''')
    

    # replacements for sql
    df = df.applymap(lambda x: x.replace("'", "\\'") if isinstance(x, str) else x)
    df = df.fillna('NULL')


    # insert new schema into db
    columns = str(df.columns.to_list()).strip('[').strip(']').replace("'", "`")
    start = db.run_select_query(f"SELECT id FROM {table_name}", None)
    start = start[-1][0] if start else 0
    for i, (_, row) in zip(range(start, start+len(df)), df.iterrows()):
        values = ', '.join([f'\'{v}\'' for v in row.values]).replace("'NULL'", "NULL")
        update = ', '.join([f"{col}='{row[col]}'" for col in set(df.columns.to_list())]).replace("'NULL'", "NULL")
        query = f"""INSERT INTO {table_name} (`id`, {columns}) 
                    VALUES ('{i+1}', {values})
                    ON DUPLICATE KEY UPDATE {update};
                    """
        db.run_insert_query(query, None)

    return 0


if __name__ == '__main__':
    project = get_redcap_project('14102 Khandwala-Radiology Imaging Services Core Lab Workflow')
    db = AMBRA_Backups.database.Database('test_db')
    backup_from(db, project, datetime.now() - timedelta(days=1))


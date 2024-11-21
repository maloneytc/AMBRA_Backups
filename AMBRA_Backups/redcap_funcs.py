import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging
from tqdm import tqdm
from bs4 import BeautifulSoup
from redcap import Project
import configparser
import sys
import json
import numpy as np

from AMBRA_Backups import utils
import AMBRA_Backups


def get_config(config_path=None):
    if config_path:
        config_file = Path(config_path)
    else:
        config_file = Path.home().joinpath(".redcap.cfg")
    if not config_file.exists():
        logging.error(f"Could not find credentials file: {config_file}")

    config = configparser.ConfigParser()
    config.read(config_file)

    return config


def get_redcap_project(proj_name, config_path=None):
    config = get_config(config_path=config_path)
    proj_config = config[proj_name]
    return Project("https://redcap.research.cchmc.org/api/", proj_config["token"])


def backup_project(project_name, url, api_key, output_dir, bckp_files=True):
    """
    Backup a REDCap project by exporting project information, metadata, records, users, roles, role assignments,
    files, and repeating instruments to specified output directory.

    Args:
        project_name (str): The name of the REDCap project.
        url (str): The URL of the REDCap project.
        api_key (str): The API key for accessing the REDCap project.
        output_dir (Path): The directory where the backup files will be saved.
        bckp_files (bool): If true, will download attached files.

    Returns:
        None
    """
    project = Project(url, api_key)

    # Info
    # ---------------
    info_out = output_dir.joinpath(f"{project_name}_info.json")
    info_json = {
        "Project Name": project_name,
        "RedCap version": str(project.export_version()),
        "Backup date": datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
    }
    with open(info_out, "w", encoding="utf-8") as f:
        json.dump(info_json, f, ensure_ascii=False, indent=4)

    # Data Dictionary
    # ---------------
    meta_json = project.export_metadata(format_type="json")
    meta_out = output_dir.joinpath(f"{project_name}_metadata.json")
    with open(meta_out, "w", encoding="utf-8") as f:
        json.dump(meta_json, f, ensure_ascii=False, indent=4)

    # Data
    # ---------------
    forms_json = project.export_records(format_type="json")
    forms_out = output_dir.joinpath(f"{project_name}_forms.json")
    with open(forms_out, "w", encoding="utf-8") as f:
        json.dump(forms_json, f, ensure_ascii=False, indent=4)

    # Users
    # ---------------
    users_json = project.export_users(format_type="json")
    users_out = output_dir.joinpath(f"{project_name}_users.json")
    with open(users_out, "w", encoding="utf-8") as f:
        json.dump(users_json, f, ensure_ascii=False, indent=4)

    # User roles
    # ---------------
    roles_json = project.export_user_roles(format_type="json")
    roles_out = output_dir.joinpath(f"{project_name}_roles.json")
    with open(roles_out, "w", encoding="utf-8") as f:
        json.dump(roles_json, f, ensure_ascii=False, indent=4)

    role_assignment_json = project.export_user_role_assignment(format_type="json")
    role_assignment_out = output_dir.joinpath(f"{project_name}_roles_assignment.json")
    with open(role_assignment_out, "w", encoding="utf-8") as f:
        json.dump(role_assignment_json, f, ensure_ascii=False, indent=4)

    # Form-event mappings
    # fem = project.export_fem()

    # Files
    # ---------------
    if bckp_files:
        files_dir = output_dir.joinpath(f"{project_name}_Files")
        if not files_dir.exists():
            files_dir.mkdir()

        meta_df = pd.DataFrame(meta_json)

        # Find fields containing files
        files = meta_df[meta_df["field_type"] == "file"]

        for file_field_name in files["field_name"].values:
            these_records = project.export_records(fields=[file_field_name])
            for record in these_records:
                if record[file_field_name] != "":
                    content, headers = project.export_file(
                        record["record_id"],
                        file_field_name,
                        event=record.get("redcap_event_name"),
                        repeat_instance=record.get("redcap_repeat_instrument"),
                    )
                    file_path = files_dir.joinpath(
                        f"{record['record_id']} - {headers['name']}"
                    )
                    if not file_path.exists():
                        with open(file_path, "wb") as fobj:
                            fobj.write(content)

    # Repeating instruments
    # ---------------
    try:
        repeating_json = project.export_repeating_instruments_events(format_type="json")
        repeating_out = output_dir.joinpath(f"{project_name}_repeating.json")
        with open(repeating_out, "w", encoding="utf-8") as f:
            json.dump(repeating_json, f, ensure_ascii=False, indent=4)
    except:
        pass


def get_project_schema(project_name, form):
    """
    returns a ready to insert dataframe of the schema of a redcap project into CRF_RedCap_Schema
    """

    # gathering
    project = get_redcap_project(project_name)
    df = pd.DataFrame(project.export_metadata())
    df = df[df["form_name"] == form]
    field_names = pd.DataFrame(project.export_field_names())
    field_names.rename(columns={"original_field_name": "field_name"}, inplace=True)
    df = pd.merge(field_names, df, on="field_name")

    # replacing
    def val_to_text(row):
        if row["field_type"] == "checkbox":
            dic = {
                op.split(",")[0].strip(): op.split(",")[1].strip()
                for op in row["select_choices_or_calculations"].split("|")
            }
            return dic[row["choice_value"]]
        else:
            return row["select_choices_or_calculations"]

    df["select_choices_or_calculations"] = df.apply(val_to_text, axis=1)

    def replace_seperators(row):
        if row["field_type"] == "radio":
            return row["select_choices_or_calculations"].replace(",", "=")
        else:
            return row["select_choices_or_calculations"]

    df["select_choices_or_calculations"] = df.apply(replace_seperators, axis=1)

    df.loc[
        (df["field_type"] == "checkbox")
        | (df["field_type"] == "radio")
        | (df["field_type"] == "yesno"),
        "data_type",
    ] = "int"
    df.loc[df["field_type"] == "text", "data_type"] = "string"

    df.loc[df["export_field_name"].str.contains("___"), "export_field_name"] = (
        df["export_field_name"] + ")"
    )
    df.loc[df["export_field_name"].str.contains("___"), "export_field_name"] = df[
        "export_field_name"
    ].str.replace("___", "(")
    df["redcap_variable"] = df["export_field_name"]

    # This question_order functionality is only approximate. Should be double checked after schena insertion
    df["question_order"] = df["export_field_name"].str.extract(r"(\d+)")

    def apply_decimals(group):
        i = 0
        for idx, row in group.iterrows():
            group.at[idx, "question_order"] = row["question_order"] + f".{(i+1):02}"
            i += 1
        return group

    df.loc[
        (df["field_type"] == "checkbox") & (df["redcap_variable"].str.startswith("q")),
        "question_order",
    ] = (
        df[
            (df["field_type"] == "checkbox")
            & (df["redcap_variable"].str.startswith("q"))
        ]
        .groupby("question_order")
        .apply(apply_decimals)
        .reset_index(level=0, drop=True)["question_order"]
    )

    # truncating and renaming
    df = df[
        [
            "form_name",
            "redcap_variable",
            "export_field_name",
            "field_label",
            "select_choices_or_calculations",
            "field_type",
            "data_type",
            "question_order",
        ]
    ]
    df.rename(
        columns={
            "form_name": "crf_name",
            "export_field_name": "data_id",
            "select_choices_or_calculations": "data_labels",
            "field_label": "question_text",
            "field_type": "question_type",
        },
        inplace=True,
    )
    df.replace({"": None, np.nan: None}, inplace=True)
    return df


def comp_schema_cap_db(db_name, project_name):
    """
    Checks for differences between
        1. data table unique redcap_variables and schema's redcap_variables
        2. question_text in live redcap and db schema
        3. radio button options in live redcap and db schema
    Any differences are added as an error string to be thrown at the start
    of a dag making a report depending on the db schema(right now just csv reports 8/19/24)
    """

    db = AMBRA_Backups.database.Database(db_name)
    project = get_redcap_project(project_name)

    forms = [f["instrument_name"] for f in project.export_instruments()]

    master_discreps = ""

    for crf_name in forms:
        # redcap_variable discrepancies
        unique_data_vars = pd.DataFrame(
            db.run_select_query(
                """SELECT DISTINCT(redcap_variable) 
            FROM CRF_RedCap
            JOIN CRF_Data_RedCap
                ON CRF_RedCap.id = CRF_Data_RedCap.id_crf
            WHERE crf_name = %s""",
                [crf_name],
                column_names=True,
            )
        )
        var_discrep_string = ""
        if not unique_data_vars.empty:
            unique_data_vars = unique_data_vars["redcap_variable"]
            schema_vars = pd.DataFrame(
                db.run_select_query(
                    """SELECT redcap_variable FROM CRF_Schema_RedCap
                WHERE crf_name = %s""",
                    [crf_name],
                    column_names=True,
                )
            )["redcap_variable"]

            var_discreps = unique_data_vars[
                ~unique_data_vars.isin(schema_vars)
            ].to_list()
            if var_discreps:
                # redcap_variables inside the data table might not have a schema variable to coorispond to, but might have an active crf_id
                # So the non-included redcap_variable will be attached to a csv report if not taken out of the data table, or have the schema corrected. Case by case
                var_discrep_string = f"\nThe following CRF_Data_RedCap.redcap_variable's are not in CRF_schema_RedCap.redcap_variable's:\n{var_discreps}\n\n"

        # print('redcap_variables')
        # print('CRF_Data_RedCap')
        # display(unique_data_vars)
        # print('CRF_Schema_RedCap')
        # display(schema_vars)

        # question text discrepancies
        schema_questions = pd.DataFrame(
            db.run_select_query(
                """SELECT question_text, redcap_variable FROM CRF_Schema_RedCap
                                        WHERE crf_name = %s AND question_text IS NOT NULL""",
                [crf_name],
                column_names=True,
            )
        )
        schema_questions["variable-value"] = (
            schema_questions["redcap_variable"] + schema_questions["question_text"]
        )

        api_questions = pd.DataFrame(project.export_metadata())
        api_questions = api_questions[api_questions["form_name"] == crf_name]
        field_names = pd.DataFrame(project.export_field_names())
        field_names.rename(columns={"original_field_name": "field_name"}, inplace=True)
        api_questions = pd.merge(
            api_questions, field_names, on="field_name", how="left"
        )
        api_questions.loc[
            api_questions["export_field_name"].str.contains("___", na=False),
            "export_field_name",
        ] = api_questions.loc[
            api_questions["export_field_name"].str.contains("___", na=False),
            "export_field_name",
        ].apply(lambda x: x.split("___")[0] + "(" + x.split("___")[1] + ")")
        api_questions["redcap_variable"] = api_questions["export_field_name"]

        def only_html(row):
            soup = BeautifulSoup(row["field_label"], "html.parser")
            if bool(soup.find()):
                return row["field_label"]

        master_html = "".join(
            api_questions.apply(only_html, axis=1).dropna().values.tolist()
        )
        api_questions = api_questions[
            (api_questions["field_type"] != "descriptive")
            & ~(api_questions["field_label"].str.contains("record", case=False))
            & (~api_questions["field_name"].apply(lambda x: x in master_html))
        ][["redcap_variable", "field_label"]]
        api_questions["variable-value"] = (
            api_questions["redcap_variable"] + api_questions["field_label"]
        )

        question_discreps = api_questions[
            ~api_questions["variable-value"].isin(schema_questions["variable-value"])
        ]
        ques_discrep_string = ""
        if not question_discreps.empty:
            discrep_dict = {v[0]: v[1] for v in question_discreps.values}
            ques_discrep_string = f"\nThe following api-metadata question_text's are not in CRF_Schema_RedCap.question_text's:\n{{redcap_variable : question_text}}\n\n{discrep_dict}\n\n"

        # print('question_text')
        # print('schema_questions')
        # display(schema_questions)
        # print('api_questions')
        # display(api_questions.reset_index())

        # radio button option discrepancies
        schema_radio_options = pd.DataFrame(
            db.run_select_query(
                """SELECT * FROM CRF_Schema_RedCap
                                    WHERE crf_name = %s AND question_type = 'radio'""",
                [crf_name],
                column_names=True,
            )
        )
        radio_discrep_string = ""
        if not schema_radio_options.empty:
            schema_radio_options = schema_radio_options["data_labels"]

            def schema_rep_seps(string_ops):
                return "|".join(
                    [
                        ss.split("=")[0].strip()
                        + "="
                        + "=".join(ss.split("=")[1:]).strip()
                        for ss in string_ops.split("|")
                    ]
                )

            schema_radio_options = schema_radio_options.apply(schema_rep_seps)

            api_radio_options = pd.DataFrame(project.metadata)
            api_radio_options = api_radio_options[
                (api_radio_options["form_name"] == crf_name)
                & (api_radio_options["field_type"] == "radio")
            ][["field_name", "select_choices_or_calculations"]]

            def api_rep_seps(string_ops):
                return "|".join(
                    [
                        ss.split(",")[0].strip()
                        + "="
                        + ",".join(ss.split(",")[1:]).strip()
                        for ss in string_ops.split("|")
                    ]
                )

            api_radio_options["select_choices_or_calculations"] = api_radio_options[
                "select_choices_or_calculations"
            ].apply(api_rep_seps)

            radio_discreps = api_radio_options[
                ~api_radio_options["select_choices_or_calculations"].isin(
                    schema_radio_options
                )
            ]
            if not radio_discreps.empty:
                discrep_dict = {v[0]: v[1] for v in radio_discreps.values}
                radio_discrep_string = f"The following api-metadata radio button options's are not in CRF_Schema_RedCap.data_labels's(radio button options):\n{{redcap_variable : select_choices_or_calculations}}\n\n{discrep_dict}\n"

        # print('radio button options')
        # print('schema_radio_options')
        # display(schema_radio_options)
        # print('api_radio_options')
        # display(api_radio_options.reset_index()

        form_discrepancies = (
            var_discrep_string + ques_discrep_string + radio_discrep_string
        )
        if form_discrepancies:
            master_discreps += f"\n{crf_name:-^{40}}\n{form_discrepancies}"

    if master_discreps:
        print("====================================================================")
        print("====================================================================")
        print(master_discreps)
        print("====================================================================")
        print("====================================================================")
        raise Exception("Please handle the above discrepancies")


def details_to_dict(log_details):
    """
    Converts log details string into dictionary of questions and values
    Splits on '=' because ',' can be used in comment fields, so not reliable to split on
    """

    # repalcements
    log_details = log_details.replace("unchecked", "0").replace("checked", "1")

    questions = {}
    strings = log_details.split("=")

    if len(strings) == 2:
        questions[strings[0].strip()] = strings[1].strip().strip("'")
        return questions

    for i in range(0, len(strings) - 1):
        if i == 0:
            questions[strings[i].strip()] = ",".join(
                strings[i + 1].split(",")[:-1]
            ).strip()
        elif i == len(strings) - 2:
            questions[strings[i].split(",")[-1].strip()] = strings[i + 1].strip()
        else:
            questions[strings[i].split(",")[-1].strip()] = ",".join(
                strings[i + 1].split(",")[:-1]
            ).strip()

    # removing extra 's from questions without removing purposeful 's
    for question in questions:
        questions[question] = questions[question].strip("'")

    return questions


def grab_logs(db, project, only_record_logs, start_date=None, end_date=None):
    """
    Extracts logs from redcap from start_date to end_date
    If only_record_logs is true only logs that modify records are extracted
    """
    if start_date is None:
        start_date = db.run_select_query("""SELECT * FROM backup_info_RedCap""")
        if (
            len(start_date) == 0
        ):  # if new project without any backup info, start from 2000, all data
            start_date = datetime(2000, 1, 1)
        else:
            start_date = start_date[0][1]
    if end_date is None:
        end_date = datetime.now()
    
    if not only_record_logs:
        logs = project.export_logging(begin_time=start_date, end_time=end_date)
   
    else:
        log_add = project.export_logging(
            begin_time=start_date, 
            end_time=end_date, 
            log_type='record_add',
        )
        log_edit = project.export_logging(
            begin_time=start_date, 
            end_time=end_date, 
            log_type='record_edit',
        )
        log_delete = project.export_logging(
            begin_time=start_date, 
            end_time=end_date, 
            log_type='record_delete',
        )

        logs = log_add + log_delete + log_edit
        logs.sort(key=lambda log: datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M'))

    return logs

def extract_details(details):
    '''
    Extract a dictionary details from log['details']

    - left right pointer
    - left pointer indicates start of info, right pointer end of info
    - keep a list of stuff like quotes to keep in track the closign quotes
    - right pointer must be at the ending quote (except when it's the ~checked~ case
    - set left pointer to be at next thing always
    '''
    
    n = len(details)
    details_dict = dict()
    if n == 1:
        return details_dict
    l = 0
    r = 1

    while r < n:
        # If the current var is [instance = int]
        if details[l] == '[':
            check = details[l:l+12]
            if check == '[instance = ':
                substring = details[l:]
                start = substring.index('= ') + 2 + l
                end = substring.index(']') + l
                details_dict['[instance]'] = int(details[start:end])
                r = end + 1
            else:
                raise Exception("This case should not be possible")
        
        # For regular variables 
        else:
            # Extract variable
            substring = details[l:]
            end_var = substring.index(' = ') + l
            variable = details[l:end_var]

            # Find value attached to variable
            start_val = end_var + 3
            r = start_val + 1

            if details[start_val] == "'":
                found_val = False
                while not found_val:
                    if r == n:
                        found_val = True
                        continue
                    current_r = details[r]
                    
                    # If found potential enclosing single quote
                    if current_r == "'": 
                        # Check if next character is a comma (eg `q1001 = '2', q1002 = '3'`)
                        next_chr = details[r+1]
                        r += 1

                        # If comma and correct number of quotes so far, then assume enclosing quote
                        if next_chr == ",":
                            found_val = True
                            continue
                    # Else keep going
                    else:
                        r += 1

            # For cases like `q1003 = checked`
            else:
                substring = details[start_val:]
                r = substring.index(',') + start_val
            
            val = details[start_val:r]
            details_dict[variable] = val

        l = r + 2
        r = l + 1

    return details_dict
                                

def export_records_wrapper(project, patient_name, crf_name, instance=None):
    """
    wrapper is necessary because of a export_record bug. If a repeating instance form is
    the first form in the project, a residual row is returned for other forms. This function excludes
    that residual.
    Also included an instance parameter
    """
    form_df = pd.DataFrame(
        project.export_records(records=[patient_name], forms=[crf_name])
    )

    if form_df.empty:
        return form_df
    form_df = form_df[form_df[crf_name + "_complete"] != ""]
    if instance:
        if "redcap_repeat_instrument" not in form_df.columns:
            raise ValueError(f"""Project '{project.export_project_info()['project_title']}' does not have repeat instances.
                               \npatient_name: {patient_name}, crf_name: {crf_name}""")
        
        instances_list = form_df["redcap_repeat_instance"].to_list()
        if instances_list == [''] or instances_list == []:
            # If there are no instances return the empty form.
            return form_df
        
        if instance not in instances_list:
            raise ValueError(f"""Instance: {instance} not of available instances: {form_df['redcap_repeat_instance'].to_list()}
                               \nIn project: {project.export_project_info()['project_title']}, crf_name: {crf_name}, patient_name: {patient_name}""")
        form_df = form_df[form_df["redcap_repeat_instance"] == instance]
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

    project_name = project.export_project_info()["project_title"].strip()
    db_backup_proj_name = db.run_select_query(
        "SELECT project_name FROM backup_info_RedCap"
    )
    if not db_backup_proj_name:
        db.run_insert_query(
            "INSERT INTO backup_info_RedCap (project_name) VALUES (%s)", [project_name]
        )
    elif len(db_backup_proj_name) > 1:
        poss_db_names = [name[0] for name in db_backup_proj_name]
        if project_name not in poss_db_names:
            raise ValueError(
                f"Live redcap name: {project_name}, is not in list of backup names: {poss_db_names}"
            )
    else:
        db_backup_proj_name = db_backup_proj_name[0][0]
        if project_name != db_backup_proj_name:
            raise ValueError(
                f"Live redcap name: {project_name}, database backup name: {db.db_name}.{db_backup_proj_name}"
            )

    start_date = db.run_select_query(
        """SELECT last_backup FROM backup_info_RedCap WHERE project_name = %s""",
        [db_backup_proj_name],
    )
    if not start_date:
        start_date = datetime(1900, 1, 1)
        db.run_insert_query(
            """INSERT INTO backup_info_RedCap (last_backup) VALUES (%s)""",
            [start_date],
        )
    else:
        start_date = start_date[0][0]

    only_record_logs = True 
    record_logs = grab_logs(db, project, only_record_logs, start_date, end_date)

    # dictionary of form names and their variables
    master_form_var_dict = {}
    for var in project.metadata:
        if var["field_name"] == "record_id":
            continue  # not necessary. record_id will be created at creation of a patient in redcap
        if var["form_name"] not in master_form_var_dict:
            master_form_var_dict[var["form_name"]] = [var["field_name"]]
        else:
            master_form_var_dict[var["form_name"]].append(var["field_name"])
    for form in [f["instrument_name"] for f in project.export_instruments()]:
        master_form_var_dict[form].append(f"{form}_complete")

    # repeating form collection
    form_names = [form["instrument_name"] for form in project.export_instruments()]
    repeating_forms = []
    if project.export_project_info()["has_repeating_instruments_or_events"] == 1:
        rep_forms = [
            form["form_name"] for form in project.export_repeating_instruments_events()
        ]
        for name in form_names:
            if name in rep_forms:
                repeating_forms.append(name)

    # loop through record_logs and add to db
    failed_to_add = []
    for i, log in tqdm(
        enumerate(record_logs), total=len(record_logs), desc="Adding data logs to db"
    ):
        if log["details"] == "":  # no changes to record
            continue
        # log deleting a record
        if "Delete record" in log["action"]:
            patient_name = log["action"].split(" ")[-1].strip()
            patient_id = str(
                db.run_select_query(
                    f"""SELECT id FROM patients WHERE patient_name = %s""",
                    [patient_name],
                )[0][0]
            )
            db.run_insert_query(
                f"""UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s""",
                [patient_id],
            )
            continue

        # list of current patients in db to check if there is a new patient
        patient_name = log["action"].split(" ")[-1].strip()
        patient_id = db.run_select_query(
            """SELECT id FROM patients WHERE patient_name = %s""", [patient_name]
        )
        if not patient_id:
            patient_id = db.run_insert_query(
                f"""INSERT INTO patients (patient_name, patient_id) VALUES (%s, %s)""",
                [patient_name, patient_name],
            )
        else:
            patient_id = patient_id[0][0]

        # Process log details from string into dictionary.
        instance = None
        details = extract_details(log["details"] + ',')
        crf_name = None

        # Get CRF
        for form, vars in master_form_var_dict.items():
            for form_var in vars:
                regex = rf"^{form_var}(\([a-zA-z0-9]*\.?[a-zA-z0-9]*\))?$" # Handles multi choice var
                for detail_var in details:
                    if re.fullmatch(regex, detail_var):
                        crf_name = form
        if not crf_name:
            failed_to_add.append(
                (patient_name, log["timestamp"], f"redcap_variables: {log}")
            )
            continue

        if (instance is None) and (crf_name in repeating_forms):
            instance = 1

        crf_row = pd.DataFrame(
            db.run_select_query(
                f"""SELECT * FROM CRF_RedCap WHERE id_patient = {patient_id} AND crf_name = \'{crf_name}\' 
                                    AND instance {'IS NULL' if instance is None else f'= {instance}'} AND deleted = '0'""",
                column_names=True,
            )
        )  # cant use run_select_query.record here, because ('IS NULL' or '= #') is not a valid sql variable
        record_df = export_records_wrapper(project, patient_name, crf_name, instance)

        if record_df.empty and crf_row.empty:  # deleted record in redcap not in db
            continue

        elif record_df.empty and not crf_row.empty:  # deleted record in redcap in db
            deleted = 1
            db.run_insert_query(
                """UPDATE CRF_RedCap SET deleted = %s WHERE id = %s""",
                [deleted, str(crf_row["id"].iloc[0])],
            )

        elif not record_df.empty:  # data to enter
            # preprocess record_df for data insertion/update
            irrelevant_columns = {
                "redcap_repeat_instrument",
                "redcap_event_name",
                "redcap_repeat_instance",
            }
            record_df = record_df.drop(irrelevant_columns, axis=1, errors="ignore")
            record_df = record_df.melt(var_name="redcap_variable")
            record_df.loc[
                record_df["redcap_variable"].str.contains("___"), "redcap_variable"
            ] = record_df["redcap_variable"] + ")"
            record_df.loc[
                record_df["redcap_variable"].str.contains("___"), "redcap_variable"
            ] = record_df["redcap_variable"].str.replace("___", "(")

            if not crf_row.empty:  # update
                if f"{crf_name}_status" in record_df['redcap_variable'].to_list():
                    if (
                        record_df.loc[record_df['redcap_variable'] == f"{crf_name}_status", 'value'].iloc[0] == "4"
                        or record_df.loc[record_df['redcap_variable'] == f"{crf_name}_status", 'value'].iloc[0] == "5"
                    ):
                        verified = 1
                        db.run_insert_query(
                            """UPDATE CRF_RedCap SET verified = %s WHERE id = %s""",
                            [verified, str(crf_row["id"].iloc[0])],
                        )
                crf_id = crf_row["id"].iloc[0]
                record_df["id_crf"] = crf_id

                db_vars = db.run_select_query("""SELECT redcap_variable FROM CRF_Data_RedCap WHERE id_crf = %s""", [crf_id.item()])
                db_vars = [v[0] for v in db_vars]
                for _, row in record_df.iterrows():

                    if row['redcap_variable'] in db_vars:
                        db.run_insert_query(
                            "UPDATE CRF_Data_RedCap SET value = %s WHERE id_crf = %s AND redcap_variable = %s",
                            [row["value"], crf_id.item(), row["redcap_variable"]],
                        )
                    else:
                        # this condition is from a previous method of inserting into the database only using logs. 
                        # The new(current 10/30/24) method initializes data into the data table with every value, the logs only used fields that were filled out.
                        # after api initializations crf data, the data is only updated, not inserted. So existing crf data before this implementation will never
                        # have their new values inserted, thus this else condition inserts the missing data
                        db.run_insert_query(
                            """INSERT INTO CRF_Data_RedCap (id_crf, value, redcap_variable) VALUES (%s, %s, %s)""",
                            [crf_id.item(), row["value"], row["redcap_variable"]],
                        )

            elif crf_row.empty:  # insert
                deleted = 0
                verified = 0
                if f"{crf_name}_status" in record_df['redcap_variable'].to_list():
                    if (
                        record_df.loc[record_df['redcap_variable'] == f"{crf_name}_status", 'value'].iloc[0] == "4"
                        or record_df.loc[record_df['redcap_variable'] == f"{crf_name}_status", 'value'].iloc[0] == "5"
                    ):
                        verified = 1
                crf_id = db.run_insert_query(
                    """INSERT INTO CRF_RedCap (id_patient, crf_name, instance, deleted, verified)
                                    VALUES (%s, %s, %s, %s, %s)""",
                    [patient_id, crf_name, instance, deleted, verified],
                )
                record_df["id_crf"] = crf_id

                # insert record df rows into CRF_Data_RedCap
                utils.df_to_db_table(db, record_df, "CRF_Data_RedCap")

    # After trying to add all the logs, if there are any logs with questions not attached
    # to a current crf (outdated variable), they will be printed to an error string
    if failed_to_add:
        failed_string = ""
        failed_string = failed_string + "------------------------------------\n"
        failed_string = (
            failed_string + f'data export date: {datetime.now().strftime("%Y-%m-%d")}\n'
        )
        for log in failed_to_add:
            failed_string = failed_string + "------------\n"
            failed_string = failed_string + f"Patient_name: {log[0]}\n"
            failed_string = failed_string + f"Date of redcap entry: {log[1]}\n"
            failed_string = failed_string + f"REDCap variable(s) : {log[2]}\n"

        # this class is a solution I found to get the failed_string to print newline characters
        class KeyErrorMessage(str):
            def __repr__(self):
                return str(self)

        msg = KeyErrorMessage(failed_string)
        raise KeyError(msg)

    # except Exception as e:
    #     print(f"Error backing up RedCap data: {e}")

    db.run_insert_query(
        "UPDATE backup_info_RedCap SET last_backup = %s WHERE project_name = %s",
        [datetime.now(), project_name],
    )


# using main for testing purposes, manual backups
if __name__ == "__main__":
    import AMBRA_Backups
    import AMBRA_Utils

    testing = 0
    db_name = 'CAPTIVA'
    project_name = 'CAPTIVA DC'
    # db_name = 'SISTER'
    # project_name = '29423 Vagal - SISTER'
    if testing:
        db = AMBRA_Backups.database.Database("CAPTIVA_Test")
        project = get_redcap_project(
            "14102 Khandwala-Radiology Imaging Services Core Lab Workflow"
        )
    else:
        db = AMBRA_Backups.database.Database(db_name)
        project = get_redcap_project(project_name)

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # manual backup
    # start_date = datetime(2023, 1, 1)
    # db.run_insert_query("""UPDATE backup_info_RedCap SET last_backup = %s""", [start_date])
    # start_date = datetime(2020, 7, 9, 11, 30)
    # end_date = datetime(2024, 7, 1, 13, 41)
    # project_data_to_db(db, project, start_date)

    # inserting logs only for select patient
    # project = AMBRA_Backups.redcap_funcs.get_redcap_project('CAPTIVA Data Collection')
    # logs = AMBRA_Backups.redcap_funcs.grab_logs(db, project, 1, start_date)
    # dates = []
    # for log in logs:
    #     if log['record'] == '1006':
    #         dates.append((datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M')+ timedelta(minutes=1), datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M') - timedelta(minutes=1)))

    # for date in dates:
    #     project_data_to_db(db, project, date[1], date[0])

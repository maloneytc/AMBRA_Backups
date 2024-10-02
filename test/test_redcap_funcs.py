"""
Simple Test Module for redcap_funcs
"""

import AMBRA_Backups
from datetime import datetime
import pytest
import random
import string


@pytest.fixture
def db():
    db_name = "playground"
    db = AMBRA_Backups.database.Database(db_name)
    return db


@pytest.fixture
def project():
    project_name = "14102 Khandwala-Radiology Imaging Services Core Lab Workflow"
    project = AMBRA_Backups.redcap_funcs.get_redcap_project(project_name)
    return project


def generate_random_input(size=6, chars=string.ascii_uppercase + string.digits):
    return "".join(random.choice(chars) for _ in range(size))


def get_id_patient(db, patient_name):
    patient_query = f"SELECT id FROM patients WHERE patient_name = '{patient_name}'"
    patient = db.run_select_query(patient_query)

    # add patient to db if not found
    if not patient:
        db.run_insert_query(
            "INSERT INTO patients (patient_name, patient_id) VALUES (%s, %s)",
            [patient_name, patient_name],
        )
        patient = db.run_select_query(patient_query)

    id_patient = patient[0][0]
    crf_redcap_found = db.run_select_query(
        f"SELECT id FROM CRF_RedCap WHERE id_patient = {id_patient}"
    )

    if not crf_redcap_found:
        db.run_insert_query(
            """INSERT INTO CRF_RedCap (id_patient, crf_name, record_created, record_updated, deleted) 
            VALUES (%s, 'baseline_brain_crf', %s, %s, %s)""",
            [id_patient, datetime.now(), datetime.now(), 0],
        )
    return id_patient


"""
project_data_to_db

Scenarios:
- delete_record: when log indicates a record has been deleted
- update_record_in_redcap_not_db: 
"""


def test_delete_record(mocker, db, project):
    """When a record is deleted from RedCap"""
    # patch grab_logs
    patient_name = "testPatientName"
    mock_logs = [
        {
            "timestamp": "2024-09-24 11:11",
            "username": "test_user",
            "action": f"Delete record {patient_name}",
            "details": "Deleted record",
            "record": patient_name,
        }
    ]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=mock_logs)

    # get patient id
    id_patient = get_id_patient(db, patient_name)

    # run function and check if all patients' rows have deleted = 1
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)
    patient_crf_data = db.run_select_query(
        f"""SELECT deleted FROM CRF_RedCap WHERE id_patient = {id_patient}"""
    )
    print("crf data: ", patient_crf_data)

    for data in patient_crf_data:
        assert data[0] == 1

    # set deleted back to 0 for future tests
    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 0 WHERE id_patient = %s", [id_patient]
    )


def test_update_record_not_redcap_in_db(mocker, db, project):
    # patch grab_logs
    patient_name = 10016
    choices = ["1", "2", "3"]
    text_input = generate_random_input()
    dropdown_input = random.choice(choices)
    radio_input = random.choice(choices)
    to_mock = {
        "record_id": patient_name,
        "pytest_text": text_input,
        "pytest_dropdown": dropdown_input,
        "pytest_radio": radio_input,
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": "please_dont_edit_form_for_testing",
    }

    mock_logs = [
        {
            "timestamp": "2024-09-24 11:11",
            "username": "test_user",
            "action": f"Update record {patient_name}",
            "details": f"""record_id = '{patient_name}', pytest_dropdown = '{dropdown_input}', pytest_radio = '{radio_input}'""",
            "record": patient_name,
        }
    ]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=mock_logs)

    # update record to add to project's Logging
    project.import_records([to_mock])

    # delete the record from project
    project.delete_records([patient_name])

    # get patient id, then set delete = 1 in db
    id_patient = get_id_patient(db, patient_name)
    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s", [id_patient]
    )

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # reinsert record into project for future testing
    test_record = {
        "record_id": patient_name,
        "pytest_text": "",
        "pytest_dropdown": "1",
        "pytest_radio": "1",
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": "please_dont_edit_form_for_testing",
    }
    project.import_records([test_record])

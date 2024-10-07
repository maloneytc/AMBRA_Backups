"""
Simple Test Module for redcap_funcs
"""

import AMBRA_Backups
from datetime import datetime
import pytest
import random
import string

pytest_form = "please_dont_edit_form_for_testing"


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
    """
    Get id of patient from patient_name. If patient_name not in patients table,
    insert new patient in patients table and new crf data in CRF_RedCap
    """
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
            """INSERT INTO CRF_RedCap (id_patient, crf_name, instance, record_created, record_updated, deleted) 
            VALUES (%s, %s, %s, %s, %s, %s)""",
            [id_patient, pytest_form, 1, datetime.now(), datetime.now(), 0],
        )
    return id_patient


"""
project_data_to_db

Scenarios:
- delete_record: when log indicates a record has been deleted
- update_record_not_redcap_not_db: when log indicates a record has been updated, but it's not found in both redcap and db
- update_record_in_redcap_not_db: when log indicates a record has been updated, it's not found in redcap but not db

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

    for data in patient_crf_data:
        assert data[0] == 1

    # set deleted back to 0 for future tests
    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 0 WHERE id_patient = %s", [id_patient]
    )

def test_update_record_not_redcap_not_db(mocker, db, project):
    """
    1. Mock log, satisfying the following conditions:
    - choose a patient name that is not in redcap, and deleted in db
    
    2. Get original patient and CRF_RedCap data before calling project_data_to_db
    - Expected behavior: the original data should be the same as the data after project_data_to_db is called ie. the
    function call does not change anything

    3. Call project_data_to_db

    4. Compare data before vs. after calling
    
    """
    # patch grab_logs
    patient_name = "test_update_not_redcap_not_db"
    choices = ["1", "2", "3"]
    dropdown_input = random.choice(choices)
    radio_input = random.choice(choices)

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

    # get patient id, then set delete = 1 in db
    id_patient = get_id_patient(db, patient_name)
    original_patient_data = db.run_select_query(
        """SELECT * FROM patients WHERE patient_name = %s""", [patient_name]
    )
    original_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{pytest_form}'
                                    AND instance = '1' AND deleted = '0'"""
    )

    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s", [id_patient]
    )

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    current_patient_data = db.run_select_query(
        """SELECT * FROM patients WHERE patient_name = %s""", [patient_name]
    )
    current_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{pytest_form}'
                                    AND instance = '1' AND deleted = '0'"""
    )

    print('\toriginal patient data:', original_patient_data, '\n\t original crf data:', original_crf_data)
    print('\tcurrent patient data:', original_patient_data, '\n\t current crf data:', current_crf_data)

    # test if all data is correct
    assert original_patient_data == current_patient_data
    assert original_crf_data == current_crf_data
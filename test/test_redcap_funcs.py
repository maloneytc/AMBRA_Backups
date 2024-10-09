"""
Simple Test Module for redcap_funcs
"""

import AMBRA_Backups
from datetime import datetime
import pytest
import random
import string
import pandas as pd

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
    patient_name = "test_delete"
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
    - Choose a patient name that is not in redcap, and deleted in db

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

    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s", [id_patient]
    )
    original_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{pytest_form}'
                                    AND instance = '1' AND deleted = '1'"""
    )

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    current_patient_data = db.run_select_query(
        """SELECT * FROM patients WHERE patient_name = %s""", [patient_name]
    )
    current_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{pytest_form}'
                                    AND instance = '1' AND deleted = '1'"""
    )

    # test if all data is correct
    assert original_patient_data == current_patient_data
    assert original_crf_data == current_crf_data

    db.run_insert_query("""DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""", [int(current_crf_data[0][0])])
    db.run_insert_query("""DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient])
    db.run_insert_query("""DELETE FROM patients WHERE patient_name = %s""", [patient_name])


def test_update_record_not_in_redcap_in_db(mocker, db, project):
    """
    Condition for mock log:
    1. Must not be a live record in redcap, but have a CRF_RedCap entry with deleted = '0'
    - In the following scenario: 
    ex. weeks logs: [..., update 9999, ..., delete 9999, ...]
    'update 9999' would try to make an api call for record 9999, but 'delete 9999' indicates the record
    is no longer present, and 9999 would already exist in the database since it is an update, 
    hence meeting the condition `record_df.empty and not crf_row.empty` 

    test:
    1. create patient and CRF_RedCap through `get_id_patient`(in_db)
    2. create a record in RedCap for patient(in_redcap)
    3. delete the same patient(not_in_redcap)
    4. create a mock update log of patient
    5. send log through `project_data_to_db`
    6. check that `project_data_to_db` set patient's CRF_RedCap deleted = '1'
    7. test complete, delete test patient, CRF_RedCap, and CRF_Data_RedCap entries
    """

    # in_db
    patient_name = 'patient_not_in_redcap_in_db'
    id_patient = get_id_patient(db, patient_name)

    # in_redcap
    choices = ["1", "2", "3"]
    dropdown_input = random.choice(choices)
    radio_input = random.choice(choices)
    mock_record = {
        "record_id": patient_name,
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": pytest_form,
        "pytest_dropdown": dropdown_input,
        "pytest_radio": radio_input,
        "pytest_text": generate_random_input(),
    }
    project.import_records([mock_record])

    # not_in_redcap
    project.delete_records(records=[patient_name])

    # mock
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

    # data download
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # check for deletion
    crf_row = pd.DataFrame(db.run_select_query(
        "SELECT * FROM CRF_RedCap WHERE id_patient = %s",
        [id_patient], column_names=True)
    )
    print(crf_row['deleted'].to_markdown())
    all_deleted = (crf_row['deleted'] == 1)
    print(all_deleted.to_markdown())
    assert (crf_row['deleted'] == 1).all()

    # clean up
    print(int(crf_row['id'].iloc[0]))
    db.run_insert_query("""DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""", [int(crf_row['id'].iloc[0])])
    db.run_insert_query("""DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient])
    db.run_insert_query("""DELETE FROM patients WHERE patient_name = %s""", [patient_name])



def test_update_record_in_redcap_not_db(mocker, db, project):
    """
    1. Mock log satisfying the following conditions:
    - Choose a patient name that is in redcap, but not found in db

    2. Change record on redcap based on mock log
    - project_data_to_db() pulls live data from redcap, so record has to be actually updated on redcap

    3. Call project_data_to_db

    4. Compare data between log vs. db
    """
    # patch grab_logs
    patient_name = "test_update_in_redcap_not_db"
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
    mock_record = {
        "record_id": patient_name,
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": pytest_form,
        "pytest_dropdown": dropdown_input,
        "pytest_radio": radio_input,
        "pytest_text": generate_random_input(),
    }

    # change the record on redcap based on mock
    project.import_records([mock_record])

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    id_patient = get_id_patient(db, patient_name)
    print('id patient:\t', id_patient)

    # check if CRF_RedCap has correct data
    record_found = db.run_select_query(
        """SELECT id, crf_name, instance, deleted, verified
        FROM CRF_RedCap
        WHERE id_patient = %s""",
        [id_patient],
    )
    record = record_found[0]

    id_crf = record[0]
    assert record[1] == pytest_form
    assert record[2] == 1 #instance
    assert record[3] == 0
    assert record[4] == 0

    # compare CRF_Data_RedCap with mock log
    data_found = db.run_select_query(
        """SELECT value, redcap_variable 
        FROM CRF_Data_RedCap
        WHERE id_crf = %s
        """,
        [id_crf],
    )

    for data in data_found:
        variable = data[1]
        value = data[0]
        if variable not in mock_record:
            assert value == "0"
        else:
            assert value == mock_record[variable]

    crf_found = db.run_select_query(
        "SELECT id FROM CRF_RedCap WHERE id_patient = %s AND crf_name = %s",
        [id_patient, pytest_form]
    )
    id_crf = crf_found[0][0]
    print('id crf\t', id_crf)

    # delete patient and crf data from db for future testing
    with db.connection.cursor() as cursor:
        cursor.execute("DELETE FROM patients WHERE id = %s", [id_patient])
        cursor.execute("DELETE FROM CRF_Data_RedCap WHERE id_crf = '%s'", [id_crf])
        cursor.execute("DELETE FROM CRF_RedCap WHERE id_patient = '%s'", [id_patient])
    db.connection.commit()


def test_update_record_in_redcap_in_db(mocker, db, project):
    """
    1. Mock log satisfying the following conditions:
    - Choose a patient name that is in both redcap and db

    2. Change record on redcap based on mock log
    - project_data_to_db() pulls live data from redcap, so record has to be actually updated on redcap

    3. Call project_data_to_db

    4. Compare data between log vs db
    """
    # patch grab_logs
    patient_name = "test_update_in_redcap_in_db"
    choices = ["1", "2", "3"]
    dropdown_input = random.choice(choices)
    radio_input = random.choice(choices)
    id_patient = get_id_patient(db, patient_name)

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
    mock_record = {
        "record_id": patient_name,
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": pytest_form,
        "pytest_dropdown": dropdown_input,
        "pytest_radio": radio_input,
        "pytest_text": generate_random_input(),
    }

    # change the record on redcap based on mock
    project.import_records([mock_record])

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # check if CRF_RedCap has correct data
    record_found = db.run_select_query(
        """SELECT id, crf_name, instance, deleted, verified
        FROM CRF_RedCap
        WHERE id_patient = %s""",
        [id_patient],
    )
    record = record_found[0]

    id_crf = record[0]
    assert record[1] == pytest_form
    assert record[2] == 1 #instance
    assert record[3] == 0
    assert record[4] == 0

    # compare CRF_Data_RedCap with mock log
    data_found = db.run_select_query(
        """SELECT value, redcap_variable 
        FROM CRF_Data_RedCap
        WHERE id_crf = %s
        """,
        [id_crf],
    )
    print('id_crf:', id_crf)

    for data in data_found:
        variable = data[1]
        value = data[0]
        if variable not in mock_record:
            assert value == "0"
        else:
            assert value == mock_record[variable]

    db.run_insert_query("""DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""", [id_crf])
    db.run_insert_query("""DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient])
    db.run_insert_query("""DELETE FROM patients WHERE patient_name = %s""", [patient_name])
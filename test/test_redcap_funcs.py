"""
Simple Test Module for redcap_funcs
"""

import AMBRA_Backups
from datetime import datetime
import pytest
import random
import string
import pandas as pd

form_input = "please_dont_edit_form_for_testing"


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
            [id_patient, form_input, 1, datetime.now(), datetime.now(), 0],
        )
    return id_patient


def create_mock(patient_name, action):
    """Create a mock log based on patient_name and action."""
    # Mock record
    choices = ["1", "2", "3"]
    dropdown_input = random.choice(choices)
    radio_input = random.choice(choices)
    text_input = generate_random_input()
    mock_record = {
        "record_id": patient_name,
        "redcap_repeat_instance": 1,
        "redcap_repeat_instrument": form_input,
        "pytest_dropdown": dropdown_input,
        "pytest_radio": radio_input,
        "pytest_text": text_input,
    }

    # Mock log
    test_user = "test_user"
    timestamp = datetime.now()
    mock_log = {
        "timestamp": timestamp,
        "username": test_user,
        "action": f"{action.title()} record {patient_name}",
        "record": patient_name,
    }
    details = ""

    skip_variables = {"redcap_repeat_instance", "redcap_repeat_instrument"}
    for var in mock_record:
        if var in skip_variables:
            continue
        details += f"{var} = '{mock_record[var]}', "

    details = details[:-2]
    mock_log["details"] = details

    return {"mock_record": mock_record, "mock_log": mock_log}


"""
Tests for project_data_to_db: pulls realtime logs and data from RedCap project to update backup database.

A. Concern
- Many scenarios to handle: 
    + Log indicates a record has been created, but record is not there in live data
    + Both log and live data indicate record's deletion, but record is there in database..

B. Solution
- Test the major scenarios
- Tests' general algorithm:
    1. Create mock logs based on scenario
    2. Import/Delete records from RedCap/database if needed
    3. Call project_data to_db()
    4. Compare data (before vs. after, mock vs. database)
    5. Clean up ie. delete test data from db

C. Terminology:
- f: project_data_to_db
"""


def test_delete_record(mocker, db, project):
    """
    Mock log conditions:
    1. "Delete record" in details

    Algorithm:
    1. Mock log
    2. Create temp patient in database
    3. Call f
    4. Check if patient data in CRF_RedCap has been deleted
    5. Clean up
    """
    # Mock log
    patient_name = "patient_delete"
    mock = create_mock(patient_name, "Delete")
    mock_log = mock["mock_log"]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=[mock_log])

    # Create patient and CRF patient
    id_patient = get_id_patient(db, patient_name)

    # Call f
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)
    patient_crf_data = db.run_select_query(
        f"""SELECT deleted FROM CRF_RedCap WHERE id_patient = {id_patient}"""
    )

    # Check if all patients' rows have deleted = 1
    for data in patient_crf_data:
        assert data[0] == 1

    # Delete patient and patient's data
    db.run_insert_query(
        """DELETE FROM patients WHERE patient_name = %s""", [patient_name]
    )
    db.run_insert_query(
        """DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient]
    )


def test_update_record_not_redcap_not_db(mocker, db, project):
    """
    Mock log conditions:
    1. "Update record" in details
    2. Patient data not found in both live RedCap and database

    Algorithm:
    1. Mock log
    2. Create temp patient and crf patient in database
    3. Set deleted = 1 for crf patient in database
    4. Call f
    5. Compare database before vs. after calling f
    - Expected behavior: should be the same since f should not change anything
    6. Clean up
    """
    # Mock log
    patient_name = "patient_not_redcap_not_db"
    mock = create_mock(patient_name, "Update")
    mock_log = mock["mock_log"]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=[mock_log])

    # Create patient and CRF patient
    id_patient = get_id_patient(db, patient_name)
    original_patient_data = db.run_select_query(
        """SELECT * FROM patients WHERE patient_name = %s""", [patient_name]
    )

    # Set deleted = 1 for CRF patient
    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 1 WHERE id_patient = %s", [id_patient]
    )
    original_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{form_input}'
                                    AND instance = '1' AND deleted = '1'"""
    )

    # Call f
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # Get current data from database
    current_patient_data = db.run_select_query(
        """SELECT * FROM patients WHERE patient_name = %s""", [patient_name]
    )
    current_crf_data = db.run_select_query(
        f"""SELECT * FROM CRF_RedCap WHERE id_patient = {id_patient} AND crf_name = '{form_input}'
                                    AND instance = '1' AND deleted = '1'"""
    )

    # Compare database before vs. after calling f
    assert original_patient_data == current_patient_data
    assert original_crf_data == current_crf_data

    db.run_insert_query(
        """DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""",
        [int(current_crf_data[0][0])],
    )
    db.run_insert_query(
        """DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient]
    )
    db.run_insert_query(
        """DELETE FROM patients WHERE patient_name = %s""", [patient_name]
    )


def test_update_record_not_redcap_in_db(mocker, db, project):
    """
    Condition for mock log:
    1. Must not be a live record in redcap, but have a CRF_RedCap entry with deleted = '0'
    - In the following scenario:
    ex. weeks logs: [..., update 9999, ..., delete 9999, ...]
    'update 9999' would try to make an api call for record 9999, but 'delete 9999' indicates the record
    is no longer present, and 9999 would already exist in the database since it is an update,
    hence meeting the condition `record_df.empty and not crf_row.empty`

    Algorithm:
    1. Create patient and CRF_RedCap through `get_id_patient`(in_db)
    2. Create a record in RedCap for patient(in_redcap)
    3. Delete the same patient(not_in_redcap)
    4. Create a mock update log of patient
    5. Send log through `project_data_to_db`
    6. Check that `project_data_to_db` set patient's CRF_RedCap deleted = '1'
    7. Clean up
    """

    # in_db
    patient_name = "patient_not_redcap_in_db"
    id_patient = get_id_patient(db, patient_name)

    # Mock
    mock = create_mock(patient_name, "Update")
    mock_log = mock["mock_log"]
    mock_record = mock["mock_record"]

    project.import_records([mock_record])
    # not_in_redcap
    project.delete_records(records=[patient_name])

    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=[mock_log])

    # Call f
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # Check for deletion
    crf_row = pd.DataFrame(
        db.run_select_query(
            "SELECT * FROM CRF_RedCap WHERE id_patient = %s",
            [id_patient],
            column_names=True,
        )
    )
    assert (crf_row["deleted"] == 1).all()

    # Clean up
    db.run_insert_query(
        """DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""",
        [int(crf_row["id"].iloc[0])],
    )
    db.run_insert_query(
        """DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient]
    )
    db.run_insert_query(
        """DELETE FROM patients WHERE patient_name = %s""", [patient_name]
    )


def test_update_record_in_redcap_not_db(mocker, db, project):
    """
    Condition for mock log:
    1. "Update record" in details
    2. A live record in RedCap, but not found in database

    Algorithm:
    1. Mock log
    2. Update/Create record on RedCap based on mock log
    3. Delete patient CRF from database
    3. Call f
    4. Compare data between mock vs. db
    """
    # Mock
    patient_name = "patient_update_in_redcap_not_db"
    mock = create_mock(patient_name, "Update")
    mock_log = mock["mock_log"]
    mock_record = mock["mock_record"]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=[mock_log])

    # Change the record on redcap based on mock
    project.import_records([mock_record])

    # Create patient and CRF patient
    id_patient = get_id_patient(db, patient_name)

    # Delete patient CRF from CRF_RedCap
    db.run_insert_query("DELETE FROM CRF_RedCap WHERE id_patient = %s", [id_patient])

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # Check if CRF_RedCap has correct data
    record_found = db.run_select_query(
        """SELECT id, crf_name, instance, deleted, verified
        FROM CRF_RedCap
        WHERE id_patient = %s""",
        [id_patient],
    )
    record = record_found[0]

    id_crf = record[0]
    assert record[1] == form_input
    assert record[2] == 1
    assert record[3] == 0
    assert record[4] == 0

    # Check if CRF_Data_RedCap has correct data
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
        [id_patient, form_input],
    )
    id_crf = crf_found[0][0]

    # Clean up
    db.run_insert_query("DELETE FROM patients WHERE id = %s", [id_patient])
    db.run_insert_query("DELETE FROM CRF_Data_RedCap WHERE id_crf = '%s'", [id_crf])
    db.run_insert_query("DELETE FROM CRF_RedCap WHERE id_patient = '%s'", [id_patient])


def test_update_record_in_redcap_in_db(mocker, db, project):
    """
    Condition for mock log:
    1. "Update record" in details
    1. A live record in RedCap, found in db

    Algorithm:
    1. Mock log
    2. Update/Create record on RedCap based on mock log
    3. Call f
    4. Compare data between mock vs. db
    """
    # Mock log
    patient_name = "test_update_in_redcap_in_db"
    id_patient = get_id_patient(db, patient_name)
    mock = create_mock(patient_name, "Update")
    mock_log = mock["mock_log"]
    mock_record = mock["mock_record"]
    mocker.patch("AMBRA_Backups.redcap_funcs.grab_logs", return_value=[mock_log])

    # Change the record on redcap based on mock
    project.import_records([mock_record])

    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)

    # Check if CRF_RedCap has correct data
    record_found = db.run_select_query(
        """SELECT id, crf_name, instance, deleted, verified
        FROM CRF_RedCap
        WHERE id_patient = %s""",
        [id_patient],
    )
    record = record_found[0]

    id_crf = record[0]
    assert record[1] == form_input
    assert record[2] == 1  # instance
    assert record[3] == 0
    assert record[4] == 0

    # Check if CRF_Data_RedCap has correct data
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

    db.run_insert_query("""DELETE FROM CRF_Data_RedCap WHERE id_crf = %s""", [id_crf])
    db.run_insert_query(
        """DELETE FROM CRF_RedCap WHERE id_patient = %s""", [id_patient]
    )
    db.run_insert_query(
        """DELETE FROM patients WHERE patient_name = %s""", [patient_name]
    )

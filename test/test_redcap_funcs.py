"""
Simple Test Module for redcap_funcs
"""
import AMBRA_Backups
from datetime import datetime
import pytest

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


def test_project_data_to_db_delete(mocker, db, project):
    '''When a record is deleted from RedCap'''
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

    # add test data into db (if needed)
    patient_query = f"SELECT id FROM patients WHERE patient_name = '{patient_name}'"
    patient_found = db.run_select_query(patient_query)

    if not patient_found:
        db.run_insert_query(
            "INSERT INTO patients (patient_name, patient_id) VALUES (%s, %s)",
            [patient_name, patient_name],
        )
        patient_found = db.run_select_query(patient_query)

    id_patient = patient_found[0][0]
    crf_redcap_found = db.run_select_query(
        f"SELECT id FROM CRF_RedCap WHERE id_patient = {id_patient}"
    )

    if not crf_redcap_found:
        db.run_insert_query(
            """INSERT INTO CRF_RedCap (id_patient, crf_name, record_created, record_updated, deleted) 
            VALUES (%s, 'baseline_brain_crf', %s, %s, %d)""",
            [id_patient, datetime.now(), datetime.now(), 0],
        )

    # run function and check if all patients' rows have deleted = 1
    AMBRA_Backups.redcap_funcs.project_data_to_db(db, project)
    patient_crf_data = db.run_select_query(
        f"""SELECT deleted FROM CRF_RedCap WHERE id_patient = {id_patient}"""
    )
    print('crf data: ', patient_crf_data)

    for data in patient_crf_data:
        assert data[0] == 1


    # set deleted back to 0 for future tests
    db.run_insert_query(
        "UPDATE CRF_RedCap SET deleted = 0 WHERE id_patient = %s", [id_patient]
    )

# def test_project_data_to_db_delete(mocker, db, project):
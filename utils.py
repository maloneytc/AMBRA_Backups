from pathlib import path
import zipfile
import logging
import pandas as pd

# ------------------------------------------------------------------------------
def extract(zip_file):
    """
    Extracts contents of the zip_file and places them into a directory with the
    same name.
    """
    zip_file = Path(zip_file)
    assert zip_file.exists()

    extraction_directory = zip_file.parent.joinpath(zip_file.stem)
    if extraction_directory.exists():
        logging.warning(f'Extracted directory {extraction_directory} already exists. Extraction skipped.')
    else:
        logging.info(f'Extracting zip file to {extraction_directory}')
        with zipfile.ZipFile(zip_file, 'r') as zip:
            zip.extract_all(path=extraction_directory)

    return extraction_directory

# ------------------------------------------------------------------------------
def convert_nifti(dicom_directory):
    pass
    ## XXX:

# ------------------------------------------------------------------------------
def extract_and_convert(zip_file):
    """
    Extracts dicoms from the zip_file and converts them to nifti using dcm2nii.

    Files are extracted into a directory with the same name as the zip_file.
    The nifti files are placed into this directory as well and the extracted dicoms
    are deleted after conversion.
    """
    extract(zip_file)
    ## XXX: Still need to convert

# ------------------------------------------------------------------------------
def html_to_dataframe(html):
    """
    Extract the CRF from an html table format and export as a pandas dataframe.

    html: html as a string or a file path.
    """
    # reports_df will be a list of dataframes, one for each table in the html.
    reports_df = pd.read_html(html)

    report_df = pd.DataFrame()
    for report in reports_df:
        report = report[[0,1,3]].T
        ## XXX: Not sure if this is the right approach yet - I may not want to transpose
        # I also may want to add in the table name as an additional row/column
        ## XXX: Merge into report_df

    return report_df

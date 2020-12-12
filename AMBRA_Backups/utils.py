from pathlib import Path
import zipfile
import logging
import subprocess
import os
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
            zip.extractall(path=extraction_directory)

    return extraction_directory

# ------------------------------------------------------------------------------
def convert_nifti(dicom_directory, output_directory):
    """
    Passes the dicom_directory to dcm2niix and outputs the nifti, bids and other
    files to the output_directory path.
    """
    dcm2nii_path = Path(__file__).parent.parent.joinpath("ExternalPrograms","dcm2niix")
    if not dcm2nii_path.exists():
        raise Exception(f'Could not locate dcm2nii at {dcm2nii_path}')

    output_directory = Path(output_directory)
    if not output_directory.exists():
        os.makedirs(output_directory)


    dcm2nii = [str(dcm2nii_path), "-b", "y", "-f", "%d_%z_%s", "-z", "y", "-o", str(output_directory), str(dicom_directory)]
    subprocess.call(dcm2nii)

# ------------------------------------------------------------------------------
def extract_and_convert(zip_file, output_directory):
    """
    Extracts dicoms from the zip_file and converts them to nifti using dcm2nii.

    Files are extracted into a directory with the same name as the zip_file.
    The nifti files are placed into this directory as well and the extracted dicoms
    are deleted after conversion.
    """
    extraction_directory = extract(zip_file)
    convert_nifti(extraction_directory, output_directory)
	
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

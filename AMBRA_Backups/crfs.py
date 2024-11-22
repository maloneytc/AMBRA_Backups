"""
Extracts information from CRFs and adds to database.
"""
from datetime import datetime

from string import Template
import re
from bs4 import BeautifulSoup

import AMBRA_Backups
import AMBRA_Utils

import mysql

################################################################################
#
# Exceptions
#
################################################################################

class GetAttachmentsError(Exception):
    """
    Exception raised with an error getting an attachment from study.
    """

    # def __init__(self, study, message="Error "):
    #     self.study = study
    #     self.message = message
    #     super().__init__(self.message)
    def __init__(self, study):
        self.message = "Error getting attachements for study"
        self.study = study
        super().__init__(self.message)

    def __str__(self):
        return f'{self.message}: {self.study.study_uid}'

# ------------------------------------------------------------------------------
class GetContentError(Exception):
    """
    Exception raised on error getting attachment content.
    """
    def __init__(self, attachment):
        self.attachment = attachment
        super().__init__()

    def __str__(self):
        return f'Error getting content from attachement {self.attachment.filename}.'

# ------------------------------------------------------------------------------
class GetTitleError(Exception):
    """
    Exception raised on error getting attachment content.
    """
    def __init__(self, attachment):
        self.attachment = attachment
        super().__init__()

    def __str__(self):
        return f'Error getting title from attachement {self.attachment.filename}.'

# ------------------------------------------------------------------------------
class EncodingFormatError(Exception):
    """
    Exception raised on error encoding the response.
    """
    def __init__(self, encoding, question_id):
        self.encoding = encoding
        self.question_id = question_id
        super().__init__()

    def __str__(self):
        return f'Encoding {self.encoding} not properly formatted for question {self.question_id}'

# ------------------------------------------------------------------------------
class DecodingError(Exception):
    """
    Exception raised on error decoding the response.
    """
    def __init__(self, question_id):
        self.question_id = question_id
        super().__init__()

    def __str__(self):
        return f"Error decoding value for question {self.question_id}, please \
                    check that the variable coding is properly formatted."

# ------------------------------------------------------------------------------
class RegExpressionError(Exception):
    """
    Exception raised on regular expression error.
    """
    def __init__(self, field_value, question_id):
        self.field_value = field_value
        self.question_id = question_id
        super().__init__()

    def __str__(self):
        return f'Value {self.field_value} does not match reg. exp. pattern for \
                    question {self.question_id}'

# ------------------------------------------------------------------------------
class SpanNotFound(Exception):
    """
    Exception raised when the given span id is not found in the html CRF.
    """
    def __init__(self, span_id):
        self.span_id = span_id
        super().__init__()

    def __str__(self):
        return f'Could not find span for id: {self.span_id}'

# ------------------------------------------------------------------------------
class SchemaNotFound(Exception):
    """
    Exception raised on error getting attachment content.
    """
    def __init__(self, crf_title):
        self.crf_title = crf_title
        super().__init__()

    def __str__(self):
        return f'No schema found for {self.crf_title}'

# ------------------------------------------------------------------------------
class UnaccountedSpan(Exception):
    """
    Exception raised when a span in the CRF html is nout found in the CRF schema.
    """
    def __init__(self, span_id):
        self.span_id = span_id
        super().__init__()

    def __str__(self):
        return f'No html_span_id found in schema for {self.span_id}'

################################################################################

def get_database(database_name):
    """
    Returns a database object.
    """
    database = AMBRA_Backups.database.Database(database_name)
    return database

# ------------------------------------------------------------------------------
def get_group(ambra_account_name, group_name):
    """
    Returns the ambra group with the given name.
    """
    ambra = AMBRA_Utils.utilities.get_api()
    account = ambra.get_account_by_name(ambra_account_name)
    group = account.get_group_by_name(group_name)
    return group

# ------------------------------------------------------------------------------
def get_location(ambra_account_name, location_name):
    """
    Returns the ambra location with the given name.
    """
    ambra = AMBRA_Utils.utilities.get_api()
    account = ambra.get_account_by_name(ambra_account_name)
    location = account.get_location_by_name(location_name)
    return location

# ------------------------------------------------------------------------------
def attachment_audit(attachment):
    """
    Extracts audit information from the html attachment.
    """
    try:
        html = attachment.get_content()
    except Exception:
        print('Error getting content for ', attachment)
    soup = BeautifulSoup(html, 'html.parser')

    audit = []

    audit_spans = list(soup.find_all('span', attrs={'class':'report-audit-action'}))
    if len(audit_spans) > 0:
        for audit_span in audit_spans:
            action = audit_span.text
            user = audit_span.find_next_siblings('span',
                                        attrs={'class':'report-audit-user'})[0].text
            action_date = audit_span.find_next_siblings('span',
                                        attrs={'class':'report-audit-time'})[0].text
            audit.append({'Name':user, 'Date':action_date, 'Action':action})
    else:
        audit_spans = soup.find_all('span', attrs={'data-i18n-token':re.compile('report-audit:.*')})
        for audit_span in audit_spans:
            action = audit_span.parent.text.split(' by ')[0]
            user = audit_span.parent.text.split(' by ')[1].split(' at ')[0]
            date = audit_span.parent.text.split(' by ')[1].split(' at ')[1]

            audit.append({'Name':user, 'Date':date, 'Action':action})

    return audit

# ------------------------------------------------------------------------------
def extract_and_verify_crf_values(this_schema, soup):
    """
    Extract value from the html defined in this_schema.
    """
    question_id = this_schema["question_id"]
    html_span_id = this_schema['html_span_id']

    span = soup.find('span', attrs={'id':html_span_id})
    if span is None:
        raise SpanNotFound(html_span_id)
        #print('Could not find span for schema:', this_schema)
    field_value = span.text

    if this_schema['re_pattern']:
        pattern = re.compile(this_schema['re_pattern'])
        if not pattern.match(field_value.strip()):
            raise RegExpressionError(field_value, question_id)

    decoded_value = None

    encodings = this_schema['variable_coding']
    if encodings:
        if encodings.startswith('decode'):
            # Python Variable Decoding
            template_string = Template(encodings)
            if isinstance(field_value, str):
                to_eval = template_string.substitute(value=f"'{field_value}'")
            else:
                to_eval = template_string.substitute(value=field_value)
            try:
                # decode variable will be set in the line below
                locs={}
                exec(str(to_eval), {}, locs)
                decode = locs['decode']
            except Exception as exc:
                raise DecodingError(question_id) from exc
            decoded_value = decode
        else:
            encodings = encodings.split('/')
            for encoding in encodings:
                enc_comps = encoding.split('=')
                if len(enc_comps) >= 2:
                    if field_value == enc_comps[1]:
                        decoded_value = enc_comps[0]
                else:
                    raise EncodingFormatError(encodings, question_id)
    else:
        decoded_value = field_value

    if isinstance(decoded_value, list):
        decoded_value = ', '.join(decoded_value)

    return field_value, decoded_value

# ------------------------------------------------------------------------------
def extract_crf_values(soup, additional_fields=None):
    """
    Extract all values from the html span elements with an id field.

    Returns a list of dictionaries, one from each of the span elements.
    The dictionary keys are 'field_value', 'html_span_id', 'class', 'style'.

    Inputs:
    --------
    soup: An object of the BeautifulSoup class.

    additional_fields: list of dicts
        A dictionary of additional fields to pull from the html.
        Required fields in the dicts are 'html_span_id', 'tag', 'attrs'.
        If html_span_id is None, then it will be taken from the id attribute of
        the html element. If no html tag is found with mathcing tag and attributes,
        no data will be extracted.
    """
    all_data = []
    for span in soup.find_all('span', attrs={'id':re.compile('.*')}):
        attrs = span.attrs.copy()
        attrs['html_span_id'] = attrs.pop('id')

        html_class = attrs.get('class')
        if not html_class:
            attrs['class'] = html_class
        if isinstance(html_class, list):
            attrs['class'] = ';'.join(html_class)
        attrs['html_class'] = attrs.pop('class')

        html_style = attrs.get('style')
        if not html_style:
            attrs['style'] = html_style
        if isinstance(html_style, list):
            attrs['style'] = ';'.join(html_style)
        attrs['html_style'] = attrs.pop('style')

        field_value = span.text
        
        # The code below will grab only text at the top level of the tag and
        # will ignore text from child elements
        #field_value = ''.join(span.find_all(text=True, recursive=False)).strip()

        attrs['value'] = field_value

        all_data.append(attrs)

    if additional_fields is not None:
        for add_field in additional_fields:
            this_tag = soup.find(add_field['tag'], attrs=add_field['attrs'])
            if not this_tag is None:
                attrs = this_tag.attrs.copy()

                if add_field['html_span_id'] is None:
                    attrs['html_span_id'] = attrs.pop('id')
                else:
                    attrs['html_span_id'] = add_field['html_span_id']

                html_class = attrs.get('class')
                if not html_class:
                    attrs['class'] = html_class
                if isinstance(html_class, list):
                    attrs['class'] = ';'.join(html_class)
                attrs['html_class'] = attrs.pop('class')

                html_style = attrs.get('style')
                if not html_style:
                    attrs['style'] = html_style
                if isinstance(html_style, list):
                    attrs['style'] = ';'.join(html_style)
                attrs['html_style'] = attrs.pop('style')

                field_value = this_tag.text
                attrs['value'] = field_value

                all_data.append(attrs)

    return all_data

# ------------------------------------------------------------------------------
def crf_in_database(database, id_study, crf_id):
    """
    Check if the CRF exists in the database. If yes, returns 'True, uploaded, data_added'.
    If no, returns 'False, None, False'. Where uploaded is a datetime object and data_added is bool.
    """
    crf = database.run_select_query("""SELECT * FROM CRF WHERE id_study = %s and crf_id = %s""",
                        (id_study, crf_id), column_names=True)
    if len(crf) == 0:
        return False, None, False

    assert len(crf) == 1
    crf = crf[0]

    return True, crf['uploaded'], crf['data_added']

# ------------------------------------------------------------------------------
def set_data_added(database, id_study, crf_id, value=True):
    """
    Sets the 'data_added' field of the CRF table for the given crf with id_Study
    and crf_id to the boolean value in 'value'.
    """
    assert isinstance(value, bool)
    database.run_insert_query("""UPDATE CRF SET data_added = %s \
                                WHERE id_study = %s and crf_id = %s""",
                        (value, id_study, crf_id))

# ------------------------------------------------------------------------------
def get_schema(database, crf_title, crf_version):
    """
    Returns a list of schema dictionaries for the given crf.
    """
    schema = database.run_select_query("SELECT * FROM CRF_Schema WHERE crf_name=%s and version=%s",
                                    (crf_title, crf_version), column_names=True)
    if len(schema) == 0:
        raise SchemaNotFound(crf_title)

    return schema

# ------------------------------------------------------------------------------
def get_id_crf(database, id_study, crf_id):
    """
    Returns the id field from the CRF table for the CRF with matching id_study
    and crf_id.
    """
    id_crf = database.run_select_query("SELECT id FROM CRF WHERE id_study=%s AND crf_id=%s",
                                    record=(id_study, crf_id))
    assert len(id_crf)==1
    id_crf = id_crf[0][0]

    return id_crf

# ------------------------------------------------------------------------------
def verify_all_spans_accounted(schema, soup):
    """
    Raises UnaccountedSpan exception if all of the spans in the html are not
    included in the schema.
    """
    html_span_ids = [item.attrs.get('id') for item in
                        soup.find_all('span', attrs={'id':re.compile('.*')})]
    schema_span_ids = [this['html_span_id'] for this in schema]
    unaccounted_spans = list(set(schema_span_ids).difference(html_span_ids))

    if len(unaccounted_spans) > 0:
        raise UnaccountedSpan(unaccounted_spans)

# ------------------------------------------------------------------------------
def add_html(database, attachment, id_study, crf_version=1.0, additional_fields=None):
    """
    For html attachment add to crf table and extract data into crf_data table.
    """
    assert attachment.filename.split('.')[-1] == 'html'

    crf_id = attachment.id

    file_type = 'html'
    try:
        html = attachment.get_content()
    except Exception as exc:
        raise GetContentError(attachment) from exc
        #raise Exception('Error getting content for ', attachment)

    soup = BeautifulSoup(html, 'html.parser')

    # May not be correct for all CRFs
    #title_span = soup.find('span', attrs={'data-i18n-token':re.compile('report:.*')})
    title_span = soup.find('span', attrs={'data-i18n-token':re.compile('.*')})
    if title_span is None:
        raise GetTitleError(attachment)

    crf_title = title_span.text

    # 3) Get audit and extract the last signer
    audit = attachment_audit(attachment)

    # last signer
    signers = [this for this in audit if this['Action'] in ['Signed Addendum', 'Signed']]
    max_date = None
    last_signed = None
    signer = None
    signed_date = None
    if len(signers) > 0:
        max_date = max([this['Date'] for this in signers])
        last_signed = [this for this in signers if this['Date']==max_date][0]
        signer = last_signed['Name']
        signed_date = datetime.strptime(last_signed['Date'], '%m-%d-%Y %I:%M:%S %p')

    # 4) Add to CRF table and get id for this entry
    crf_in_db, uploaded, data_added = crf_in_database(database, id_study, crf_id)
    if not crf_in_db:
        id_crf = database.insert_dict({'id_study':id_study,
                             'crf_name':crf_title,
                             'file_type':file_type,
                             'file_name':attachment.filename,
                             'signed_by':signer,
                             'signed_date':signed_date,
                             'uploaded':attachment.uploaded,
                             'crf_id':crf_id,
                             'version':attachment.version,
                             'phi_namespace':attachment.phi_namespace,
                             'data_added': False}, 'CRF')
    elif attachment.uploaded.replace(microsecond=0) > uploaded:
        # mysql does not store microsecond resolution so have to set to 0 when comparing
        # update CRF and data
        data_added = False
        id_crf = get_id_crf(database, id_study, crf_id)
        database.update_dict({'crf_name':crf_title,
                             'file_type':file_type,
                             'file_name':attachment.filename,
                             'signed_by':signer,
                             'signed_date':signed_date,
                             'uploaded':attachment.uploaded,
                             'version':attachment.version,
                             'phi_namespace':attachment.phi_namespace,
                             'data_added': False}, 'CRF', 'id', id_crf)

    else:
        id_crf = get_id_crf(database, id_study, crf_id)

    if not data_added:
        ## Schema-less
        ## ------------
        all_data = extract_crf_values(soup, additional_fields=additional_fields)

        for this_data in all_data:
            this_data['id_crf'] = id_crf
            if len(this_data['value']) > 1024:
                this_data['value'] = this_data['value'][-1024:]

            #database.insert_dict(this_data, 'CRF_Data')
            database.run_insert_query("""
            INSERT INTO CRF_Data (id_crf, value, html_class, html_style, html_span_id)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE value=%s, html_class=%s, html_style=%s, decoded_value=NULL;
            """, (this_data['id_crf'], this_data['value'],
                   this_data['html_class'], this_data['html_style'], this_data['html_span_id'],
                   this_data['value'], this_data['html_class'], this_data['html_style']))

    set_data_added(database, id_study, crf_id, value=True)

# ------------------------------------------------------------------------------
def add_html_crfs(database, study, additional_fields=None, ignore_errors=False):
    """
    Find html CRFs in the given study and add to the database.
    """
    id_study = database.get_study_by_uid(study.study_uid)
    attachments = study.get_attachments()

    if attachments is not None:
        for attachment in attachments:
            if attachment.filename.split('.')[-1] == 'html':
                try:
                    add_html(database, attachment, id_study, additional_fields=additional_fields)
                except GetContentError as gce:
                    print(f'Could not get content from attachment {attachment.filename} \
                            from study with uid: {study.study_uid}.', gce)
                except Exception as exc:
                    if ignore_errors:
                        continue
                    raise exc

# ------------------------------------------------------------------------------
def backup_studies(database, studies, additional_html_fields=None):
    """
    Backup all studies in the given 'studies' list.
    """
    for study in studies:
        try:
            add_html_crfs(database, study, additional_fields=additional_html_fields)
        except Exception as exc:
            print(20 * '=')
            print(study)
            print(exc)
            print(20 * '=')

# ------------------------------------------------------------------------------
def backup_location_reads(database_name, ambra_account_name, location_name):
    """
    Backup html CRFs from the location namespace location_name.
    """
    database = get_database(database_name)
    location = get_location(ambra_account_name, location_name)
    studies = list(location.get_studies())

    backup_studies(database, studies)

# ------------------------------------------------------------------------------
def backup_group_reads(database_name, ambra_account_name, group_name):
    """
    Backup html CRFs from the group namespace group_name.
    """
    database = get_database(database_name)
    group = get_group(ambra_account_name, group_name)
    studies = list(group.get_studies())

    backup_studies(database, studies)

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    pass

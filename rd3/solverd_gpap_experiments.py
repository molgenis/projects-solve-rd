"""Solve-RD GPAP Experiments"""

from os import environ
from datatable import dt, f
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
from dotenv import load_dotenv
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


def get_table_attribs(pkg_entity: str = None, columns: str = None, nested_columns: str = None):
    """Retrieve a subset data from a specific table in a database
    :param pkg_entity: the name of the table in emx format (pkg_entity)
    :type pkg_entity: str

    :param nested_columns: one or more nested columns to extract when data is
        flattend (see function `flatten_data`)
    :type nested_columns: str

    :param *kwargs: additional params to pass on to `get`

    :return: dataset
    :rtype: datatable frame
    """
    data_raw = rd3.get(pkg_entity, attributes=columns, batch_size=10000)
    data_flat = flatten_data(data_raw, nested_columns)
    return dt.Frame(data_flat)


def flatten_string(value: list = None, split_at: str = ','):
    """Flatten String
    Return a comma separated string of unique values in a string

    :param value: string containing one or more comma separated values
    :type value: str

    :param split_at: character used to split string
    :type split_at: str

    :examples:
    ````
    flatten_string(value='a,b,c,c,None,d,e,f,f')
    #> 'a,b,c,d,e,f'
    ```
    """
    values = value.split(split_at)
    values_unique = {val for val in values if val != 'None'}
    return ','.join(sorted(values_unique))


def detect_record_conflicts(
        data,
        filter_col: str = None,
        value_col: str = None,
        filter_by: str = None,
        match_for: str = None,
        current_error: str = None,
        error_exclusions: list = None,
        new_error: str = None
):
    """Detect conflict between values in two different datasets by a identifier
    :param data: data containing the record to check
    :type data: datatable.frame

    :param filter_col: name of the column to filter
    :type filter_col: string

    :param value_col: name of the column that contains the vlaue to check
    :type value_col: string

    :param filter_by: value to filter reference dataset by
    :type filter_by: string

    :param filter_by: value to filter for in the reference dataset
    :type filter_by: string

    :param match_for: value to test 
    :type match_for: string

    :param current_error: current validation error
    :type current_error: string

    :param new_error: new error to add
    :type new_error: string
    """
    if filter_by is None or match_for is None:
        return current_error

    if current_error is not None and error_exclusions:
        for err in error_exclusions:
            if err in current_error:
                return current_error

    match = data[f[filter_col] == filter_by, value_col]
    if match.nrows:
        ref_value = match[0, value_col]
        if ref_value != match_for or match_for not in ref_value:
            return flatten_string(f"{current_error},{new_error}")

    return current_error

# ///////////////////////////////////////////////////////////////////////////////


# ~ 0 ~
# Get Metadata
print2('Retrieving data....')

gpap_dt = dt.Frame(rd3.get('solverdportal_experiments', batch_size=10000))
del gpap_dt['_href']

subjects_dt = get_table_attribs(
    pkg_entity='solverd_subjects',
    columns='subjectID',
    nested_columns='subjectID|id|value'
)

samples_dt = get_table_attribs(
    pkg_entity='solverd_samples',
    columns='sampleID,belongsToSubject,tissueType,ERN,organisation',
    nested_columns='subjectID|id|value'
)

experiments_dt = get_table_attribs(
    pkg_entity='solverd_labinfo',
    columns='experimentID,sampleID,capture,libraryType,library,sequencer,seqType,partOfRelease',
    nested_columns='sampleID|id|value'
)


# join metadata into one dataset
solverd_dt = experiments_dt.copy()

samples_dt.names = {'belongsToSubject': 'subjectID'}
samples_dt.key = 'sampleID'
solverd_dt = solverd_dt[:, :, dt.join(samples_dt)]

subjects_dt.key = 'subjectID'
solverd_dt = solverd_dt[:, :, dt.join(subjects_dt)]


# ///////////////////////////////////////////////////////////////////////////////


# ~ 1 ~
# Validate metadata
# Run tests on target columns and flag accordingly
print2('Validating metadata....')

# ~ 1a ~
# Do all experiments exist?
print2('Do all experiments exist in RD3?')

experiment_ids = dt.unique(solverd_dt['experimentID']).to_list()[0]

gpap_dt['error_type'] = dt.Frame([
    'unknown experiment' if value not in experiment_ids else None
    for value in gpap_dt['rdconnect_id'].to_list()[0]
])


# ~ 1b ~
# Evalute participant_id
# Does the participant exist? Is is different than the record in RD3?
print2('Is the subject ID missing?')

# is the participant ID missing?
subject_ids = dt.unique(solverd_dt['subjectID']).to_list()[0]
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f'{row[1]},missing participant')
    if row[0] is None else row[1]
    for row in gpap_dt[:, (f.participant_id, f.error_type)].to_tuples()
])


# is the participant ID unknown? I.e., does it exist in RD3?
print2('Does the subject ID exist in RD3?')
gpap_dt['error_type'] = dt.Frame([
    flatten_string(value=f"{row[1]},unknown participant")
    if row[0] is not None and row[0] not in subject_ids else row[1]
    for row in gpap_dt[:, (f.participant_id, f.error_type)].to_tuples()
])


# Does the participant ID conflict with what is in RD3?
print2('Does the subject ID conflict with the ID in RD3?')
gpap_dt['error_type'] = dt.Frame([
    detect_record_conflicts(
        data=solverd_dt,
        filter_col='experimentID',
        value_col='subjectID',
        filter_by=row[0],
        match_for=row[1],
        current_error=row[2],
        error_exclusions=[
            'unknown experiment',
            'unknown participant',
            'missing participant'
        ],
        new_error='conflicting participant'
    )
    for row in gpap_dt[:, (f.rdconnect_id, f.participant_id, f.error_type)].to_tuples()
])


# ///////////////////////////////////////

# ~ 1c ~
# Evaluate sample identifiers

# is the sample missing?
print2('Is the sample ID missing?')
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f"{row[1]},missing sample") if row[0] is None else row[1]
    for row in gpap_dt[:, (f.sample_id, f.error_type)].to_tuples()
])

# Does the sample conflict with the current record in RD3?
print2('Does the sample ID conflict with the ID in RD3?')
# gpap_dt['error_type'] = dt.Frame([
#     detect_record_conflicts(
#         data=solverd_dt,
#         filter_col='experimentID',
#         value_col='sampleID',
#         filter_by=row[0],
#         match_for=row[1],
#         current_error=row[2],
#         error_exclusions=['missing sample'],
#         new_error='conflicting sample'
#     )
#     for row in gpap_dt[:, (f.rdconnect_id, f.sample_id, f.error_type)].to_tuples()
# ])

# ///////////////////////////////////////

# finalise errors
gpap_dt['has_error'] = dt.Frame([
    bool(value) for value in gpap_dt['error_type'].to_list()[0]
])

counts = gpap_dt[:, dt.count(), dt.by(f.has_error, f.error_type)]
# gpap_dt[dt.re.match(f.error_type, '.*conflicting participant.*'), :].nrows
# gpap_dt[dt.re.match(f.error_type, '.*missing sample.*'), :].nrows
# gpap_dt[dt.re.match(f.error_type, '.*unknown experiment.*'), :].nrows
# gpap_dt[dt.re.match(f.error_type, '.*unknown participant.*'), :].nrows

# gpap_dt[dt.re.match(f.error_type, '.*conflicting sample.*'), :]

# gpap_dt[dt.re.match(f.error_type, '.*unknown experiment.*'), :][:, dt.count(), dt.by(f.subproject)]
# gpap_dt[dt.re.match(f.error_type, '.*unknown participant.*'), :][:, dt.count(), dt.by(f.subproject)]
# [:, dt.count(), dt.by(f.error_type)]

rd3.import_dt('solverdportal_experiments', gpap_dt)

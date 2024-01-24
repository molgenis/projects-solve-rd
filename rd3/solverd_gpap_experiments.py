"""Solve-RD GPAP Experiments"""

from os import environ
from datatable import dt, f
from tqdm import tqdm
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

def detect_record_conflicts(
        ref_data,
        ref_filter_col: str = None,
        ref_value_col: str = None,
        filter_by: str = None,
        match_for: str = None,
        current_error: str = None,
        error_exclusions: list = None,
):
    """Detect conflict in data
    :param subj_id: identifier of a participant
    :type subj_id: str

    :param expr_id: identifier of an experiment
    :type expr_id: str

    :param error_type: current validation error
    :type error_type: str
    """
    if filter_by is None:
        return None

    if current_error is not None and error_exclusions:
        for err in error_exclusions:
            if err in current_error:
                return current_error

    match = ref_data[f[ref_filter_col] == filter_by, ref_value_col]
    if match[0, ref_value_col] != match_for:
        return flatten_string(f"{current_error},conflicting participant")

    return current_error

# ///////////////////////////////////////////////////////////////////////////////


# ~ 1 ~
# Validate metadata
# Run tests on target columns and flag accordingly
print2('Validating metadata....')

# ~ 1a ~
# Do all experiments exist?
experiment_ids = dt.unique(solverd_dt['experimentID']).to_list()[0]

gpap_dt['error_type'] = dt.Frame([
    'unknown experiment' if value not in experiment_ids else None
    for value in gpap_dt['rdconnect_id'].to_list()[0]
])


# ~ 1b ~
# Evalute participant_id
# Does the participant exist? Is is different than the record in RD3?
subject_ids = dt.unique(solverd_dt['subjectID']).to_list()[0]

# is the participant ID missing?
gpap_dt['error_type'] = dt.Frame([
    flatten_string(
        f'{row[1]},missing participant') if row[0] is None else row[1]
    for row in gpap_dt[:, (f.participant_id, f.error_type)].to_tuples()
])


# is the participant ID unknown? I.e., does it exist in RD3?
gpap_dt['error_type'] = dt.Frame([
    flatten_string(value=f"{row[1]},unknown participant")
    if row[0] is not None and row[0] not in subject_ids else row[1]
    for row in gpap_dt[:, (f.participant_id, f.error_type)].to_tuples()
])

# Does the participant ID conflict with what is in RD3?

gpap_dt['error_type'] = dt.Frame([
    detect_conflicting_subject(
        subj_id=row[1],
        expr_id=row[0],
        error_type=row[2]
    )
    for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.participant_id, f.error_type)].to_tuples())
])


# finalise errors
gpap_dt['has_error'] = dt.Frame([
    bool(value) for value in gpap_dt['error_type'].to_list()[0]
])

# gpap_dt[:, dt.count(), dt.by(f.has_error,f.error_type)]

rd3.import_dt('solverdportal_experiments', gpap_dt)

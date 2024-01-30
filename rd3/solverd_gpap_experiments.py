"""Solve-RD GPAP Experiments"""

import re
from os import environ
from tqdm import tqdm
from datatable import dt, f
from rd3tools.molgenis import Molgenis, get_table_attribs
from rd3tools.utils import print2, as_key_pairs, recode_value
from dotenv import load_dotenv
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


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

gpap_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverdportal_experiments',
    nested_columns='name'
)

subjects_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverd_subjects',
    columns='subjectID',
    nested_columns='subjectID|id|value'
)

samples_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverd_samples',
    columns='sampleID,belongsToSubject,tissueType,ERN,organisation',
    nested_columns='subjectID|id|value'
)

experiments_dt = get_table_attribs(
    client=rd3,
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

# retrieve look up tables

erns_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverd_info_erns',
    columns='id,shortname',
)

tissues_rd3 = get_table_attribs(
    client=rd3,
    pkg_entity='solverd_lookups_tissueType',
    columns='id'
)


# ///////////////////////////////////////

# apply transforms to make validation/mapping a bit easier
# many of these transformations should only be run once

# gpap_dt['erns_rd3'] = dt.Frame([
#     None if value.lower() == 'not_applicable'
#     else re.sub(r'([-]|\s+)', '_', value.lower())
#     for value in gpap_dt['erns'].to_list()[0]
# ])

# gpap_erns = dt.unique(gpap_dt['erns_rd3']).to_list()[0]
# for value in gpap_erns:
#     if value not in erns_dt['id'].to_list()[0] and value is not None:
#         print2('ERNS', value, 'is not known')


# rd3.import_dt('solverdportal_experiments', gpap_dt)

# ///////////////////////////////////////////////////////////////////////////////

# reset errors
gpap_dt[['has_error', 'error_type']] = (False, None)

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
    for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.participant_id, f.error_type)].to_tuples())
])


# ///////////////////////////////////////

# ~ 1c ~
# Evaluate sample identifiers

# create a second sample_id column to combine novelomics and virtual samples
gpap_dt['sample_id2'] = dt.Frame([
    f"VS{row[2]}" if row[0] is None and 'SolveDF' in row[1] else row[0]
    for row in gpap_dt[:, (f.sample_id, f.subproject, f.rdconnect_id)].to_tuples()
])

# is the sample missing?
print2('Is the sample ID missing?')
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f"{row[1]},missing sample") if row[0] is None else row[1]
    for row in gpap_dt[:, (f.sample_id2, f.error_type)].to_tuples()
])

# is the sample ID unknown
sample_ids = dt.unique(solverd_dt['sampleID']).to_list()[0]
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f"{row[1]},unknown sample")
    if row[0] is not None and row[0] not in sample_ids else row[1]
    for row in gpap_dt[:, (f.sample_id2, f.error_type)].to_tuples()
])

# Does the sample conflict with the current record in RD3?
print2('Does the sample ID conflict with the ID in RD3?')
gpap_dt['error_type'] = dt.Frame([
    detect_record_conflicts(
        data=solverd_dt,
        filter_col='experimentID',
        value_col='sampleID',
        filter_by=row[0],
        match_for=row[1],
        current_error=row[2],
        error_exclusions=['missing sample'],
        new_error='conflicting sample'
    )
    for row in gpap_dt[:, (f.rdconnect_id, f.sample_id, f.error_type)].to_tuples()
])

# ///////////////////////////////////////

# validate kit

# Is kit missing?
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f"{row[1]},missing kit") if row[0] is None else row[1]
    for row in gpap_dt[:, (f.kit, f.error_type)].to_tuples()
])

# does the kit conflict with the value in RD3?
gpap_dt['error_type'] = dt.Frame([
    detect_record_conflicts(
        data=solverd_dt,
        filter_col='experimentID',
        value_col='capture',
        filter_by=row[0],
        match_for=row[1],
        current_error=row[2],
        error_exclusions=['missing kit'],
        new_error='conflicting kit'
    )
    for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.kit, f.error_type)].to_tuples())
])


# ///////////////////////////////////////

# Validate ERN assignment

# Is the ERN missing?
gpap_dt['error_type'] = dt.Frame([
    flatten_string(f"{row[1]},missing ERN") if row[0] is None else row[1]
    for row in gpap_dt[:, (f.erns_rd3, f.error_type)].to_tuples()
])

# Is the ERN value unknown?
gpap_dt['error_type'] = dt.Frame([
    row[1] if row[0] is None else (
        flatten_string(f"{row[1]},unknown ERN")
        if row[0] not in erns_dt['id'].to_list()[0] else row[1]
    )
    for row in gpap_dt[:, (f.erns_rd3, f.error_type)].to_tuples()
])

# is there a conflict between the ERN values?
gpap_dt['error_type'] = dt.Frame([
    detect_record_conflicts(
        data=solverd_dt,
        filter_col='experimentID',
        value_col='ERN',
        filter_by=row[0],
        match_for=row[1],
        current_error=row[2],
        error_exclusions=['missing ERN', 'unknown ERN'],
        new_error='conflicting ERN'
    )
    for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.erns_rd3, f.error_type)].to_tuples())
])


# ///////////////////////////////////////////////////////////////////////////////

# finalise errors
gpap_dt['has_error'] = dt.Frame([
    bool(value) for value in gpap_dt['error_type'].to_list()[0]
])

counts = gpap_dt[:, dt.count(), dt.by(f.has_error, f.error_type)]
gpap_dt[f.error_type == 'conflicting ERN', :]

rd3.import_dt('solverdportal_experiments', gpap_dt)

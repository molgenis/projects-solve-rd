"""Solve-RD GPAP Experiments"""

from os import environ
from tqdm import tqdm
from datatable import dt, f
from rd3tools.molgenis import Molgenis, get_table_attribs
from rd3tools.utils import print2
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
    values_unique = {val.strip() for val in values if val != 'None'}
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


if __name__ == '__main__':

    # ~ 0 ~
    # Get Metadata
    print2('Retrieving RD3 metadata....')

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
    print2('Joining datasets...')
    solverd_dt = experiments_dt.copy()

    samples_dt.names = {'belongsToSubject': 'subjectID'}
    samples_dt.key = 'sampleID'
    solverd_dt = solverd_dt[:, :, dt.join(samples_dt)]

    subjects_dt.key = 'subjectID'
    solverd_dt = solverd_dt[:, :, dt.join(subjects_dt)]

    # retrieve look up tables
    print2('Retrieving lookup tables...')
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

    orgs_rd3 = get_table_attribs(
        client=rd3,
        pkg_entity='solverd_info_organisations',
        columns='value'
    )

    seqtype_rd3 = get_table_attribs(
        client=rd3,
        pkg_entity='solverd_lookups_seqType',
        columns='id,label'
    )

    # reset errors
    gpap_dt[['has_error', 'error_type']] = (False, None)

    # ~ 1 ~
    # Validate metadata
    # Run tests on target columns and flag accordingly

    # Run check for missing values
    print2('Identifying records with missing values in target columns....')

    target_columns = [
        {'column': 'sample_id2', 'error': 'missing sample'},
        {'column': 'participant_id', 'error': 'missing participant'},
        {'column': 'kit', 'error': 'missing kit'},
        {'column': 'tissue', 'error': 'missing tissue'},
        {'column': 'library', 'error': 'missing library'},
        {'column': 'erns_rd3', 'error': 'missing ERN'},
        {'column': 'owner', 'error': 'missing organisation'},
        {'column': 'seq_type', 'error': 'missing seq type'}
    ]

    for target_column in target_columns:
        COLUMN_NAME = target_column['column']
        COLUMN_ERROR = target_column['error']
        if COLUMN_NAME in gpap_dt.names:
            print2('\tChecking column', COLUMN_NAME, 'for missing values....')
            gpap_dt['error_type'] = dt.Frame([
                flatten_string(f"{row[1]},{COLUMN_ERROR}")
                if row[0] is None or row[0] in ['Not_Applicable', 'None', ' ', '']
                else row[1]
                for row in gpap_dt[:, [f[COLUMN_NAME], f.error_type]].to_tuples()
            ])

    # ///////////////////////////////////////

    # ~ 1a ~
    # Do all experiments exist?
    print2('Identifying experiments that are not yet in RD3....')

    experiment_ids = dt.unique(solverd_dt['experimentID']).to_list()[0]
    gpap_dt['error_type'] = dt.Frame([
        flatten_string(f"{row[1]},unknown experiment")
        if row[0] not in experiment_ids else row[1]
        for row in gpap_dt[:, (f.rdconnect_id, f.error_type)].to_tuples()
    ])

    # ///////////////////////////////////////

    # ~ 1b ~
    # Evaluate sample identifiers
    print2('Validating sample id....')

    # is the sample ID unknown
    sample_ids = dt.unique(solverd_dt['sampleID']).to_list()[0]
    gpap_dt['error_type'] = dt.Frame([
        flatten_string(f"{row[1]},unknown sample")
        if row[0] is not None and row[0] not in sample_ids else row[1]
        for row in gpap_dt[:, (f.sample_id2, f.error_type)].to_tuples()
    ])

    # Does the sample conflict with the current record in RD3?
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

    # ~ 1c ~
    # Evalute participant_id
    print2('Validating participant id....')

    # is the participant ID unknown? I.e., does it exist in RD3?
    subject_ids = dt.unique(solverd_dt['subjectID']).to_list()[0]
    gpap_dt['error_type'] = dt.Frame([
        flatten_string(value=f"{row[1]},unknown participant")
        if row[0] is not None and row[0] not in subject_ids else row[1]
        for row in gpap_dt[:, (f.participant_id, f.error_type)].to_tuples()
    ])

    # Does the participant ID conflict with what is in RD3?
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

    # ~ 1d ~
    # Validate Kit: does the kit conflict with the value in RD3?
    print2('Validating Kit.....')

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

    # ~ 1e ~
    # Validate Tisue Types
    print2('Validating tissue types....')

    # are there any unknown tissue types?

    tissue_types = tissues_rd3['id'].to_list()[0]
    gpap_dt['error_type'] = dt.Frame([
        row[1]
        if row[1] is None else (
            flatten_string(f"{row[1]},unknown tissue")
            if row[0] not in tissue_types else row[1]
        )
        for row in gpap_dt[:, (f.tissue, f.error_type)].to_tuples()
    ])

    # Is there a conflict between tissue type values
    gpap_dt['error_type'] = dt.Frame([
        detect_record_conflicts(
            data=solverd_dt,
            filter_col='experimentID',
            value_col='tissueType',
            filter_by=row[0],
            match_for=row[1],
            current_error=row[2],
            error_exclusions=['missing tissue', 'unknown tissue'],
            new_error='conflicting tissue type'
        )
        for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.tissue, f.error_type)].to_tuples())
    ])

    # ///////////////////////////////////////

    # ~ 1f ~
    # Validate Library Strategy/SeqType (choose seq_type column)
    print2('Validating Sequencing type....')

    # Is the SeqType value unknown?
    seqtype_ids = seqtype_rd3['label'].to_list()[0]

    gpap_dt['error_type'] = dt.Frame([
        row[1] if row[0] is None else (
            flatten_string(f"{row[1]},unknown seq type")
            if row[0] not in seqtype_ids else row[1]
        )
        for row in gpap_dt[:, (f.seq_type, f.error_type)].to_tuples()
    ])

    # is there a conflict bewteen seq type values?
    gpap_dt['error_type'] = dt.Frame([
        detect_record_conflicts(
            data=solverd_dt,
            filter_col='experimentID',
            value_col='seqType',
            filter_by=row[0],
            match_for=row[1],
            current_error=row[2],
            error_exclusions=['missing seq type', 'unknown seq type'],
            new_error='conflicting seq type'
        )
        for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.library_strategy, f.error_type)].to_tuples())
    ])

    # ///////////////////////////////////////

    # ~ 1g ~
    # Validate ERN assignment
    print2('Validating ERN assignment....')
    ern_ids = erns_dt['id'].to_list()[0]

    # Is the ERN value unknown?
    gpap_dt['error_type'] = dt.Frame([
        row[1] if row[0] is None else (
            flatten_string(f"{row[1]},unknown ERN")
            if row[0] not in ern_ids else row[1]
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

    # ///////////////////////////////////////

    # ~ 1h ~
    # Validate organisation
    print2('Validating organisations....')

    # Are there any Organisation values not in RD3?
    org_ids = orgs_rd3['value'].to_list()[0]

    gpap_dt['error_type'] = dt.Frame([
        row[1] if row[0] is None else (
            flatten_string(f"{row[1]},unknown organisation")
            if row[0] not in org_ids else row[1]
        )
        for row in gpap_dt[:, (f.owner, f.error_type)].to_tuples()
    ])

    # Is there a conflict between the GPAP and RD3 organisations?
    gpap_dt['error_type'] = dt.Frame([
        detect_record_conflicts(
            data=solverd_dt,
            filter_col='experimentID',
            value_col='organisation',
            filter_by=row[0],
            match_for=row[1],
            current_error=row[2],
            error_exclusions=['missing organisation', 'unknown origanisation'],
            new_error='conflicting organisation'
        )
        for row in tqdm(gpap_dt[:, (f.rdconnect_id, f.owner, f.error_type)].to_tuples())
    ])

    # ///////////////////////////////////////////////////////////////////////////////

    # finalise errors
    print2('Preparing summary and importing....')
    gpap_dt['has_error'] = dt.Frame([
        bool(value) for value in gpap_dt['error_type'].to_list()[0]
    ])

    counts = gpap_dt[:, dt.count(), dt.by(f.has_error, f.error_type)]
    counts[f.error_type == None, 'error_type'] = 'No error'

    rd3.import_dt('solverdportal_experiments', gpap_dt)
    rd3.import_dt('solverdportal_experiment_counts', counts)

    rd3.logout()

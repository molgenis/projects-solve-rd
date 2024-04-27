""" Solve-RD Tree Mapping
FILE: rd3_data_patient_tree_mapping.py
AUTHOR: David Ruvolo
CREATED: 2022-06-20
MODIFIED: 2024-04-27
PURPOSE: mapping script for patient tree dataset
STATUS: stable
PACKAGES: **see below**
COMMENTS: see notes at the end of this script
"""

import json
from os import environ
from tqdm import tqdm
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
from datatable import dt, f
from dotenv import load_dotenv
load_dotenv()

# connect to RD3
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


def init_json(_index: int = None, subject_id: str = None, family_id: str = None):
    """Init JSON object for top-level subject metadata

    :param index: a record-level identifier (i.e., key)
    :type index: int

    :param subject_id: an identifier that distinguishes an individual
    :type subject_id: str

    :param family_id: an identifier that groups multiple related individuals
    :type family_id: str

    :return: metadata for patient tree structure
    :rtype: dict
    """
    base_url = '/menu/plugins/dataexplorer?entity=solverd_subjects'
    url = f"{base_url}&filter=subjectID%3Dq%3D{subject_id}"
    return {'id': f"RD-{_index}", 'subjectID': subject_id, 'familyID': family_id, 'group': 'patient', 'href': url}


def init_child_json(
        _index: int = None,
        _id: str = None,
        group: str = None,
        table: str = None,
        column: str = None):
    """Init Child json structure that contains a link from a unique record to
    the location in the database

    :param index: a record-level identifier (i.e., key)
    :type index: str

    :param _id: an identifier that distinguishes the record
    :type _id: str

    :param group: a value that links the current record with others
    :type group: str

    :param table: the name of a table where the data is stored
    :type table: str

    :param column: the name of the column where the record can be found
    :type column: str

    :return: metadata for a linked records
    :rtype: dict
    """
    base_url = f"/menu/plugins/dataexplorer?entity=solverd_{table}"
    url = f"{base_url}&filter={column}%3Dq%3D{_id}"
    return {'id': f"RD-{_index}", 'name': _id, 'group': group, 'href': url}


def get_table_attribs(pkg_entity: str = None, attributes: str = None, nested_columns: str = None):
    """Retrieve a subset data from a specific table in a database
    :param pkg_entity: the name of the table in emx format (pkg_entity)
    :type pkg_entity: str

    :param attributes: one or more columns to retrieve (comma-separated string)
    :type attributes: str

    :param nested_columns: one or more nested columns to extract when data is
        flattend (see function `flatten_data`)
    :type nested_columns: str

    :return: dataset
    :rtype: datatable frame
    """
    data_raw = rd3.get(pkg_entity, attributes=attributes, batch_size=10000)

    if bool(nested_columns):
        data_flat = flatten_data(data_raw, 'subjectID')
        return dt.Frame(data_flat)

    return dt.Frame(data_raw)


if __name__ == '__main__':

    # retrieve metadata
    print2('Fetching metadata...')
    subjects_dt = get_table_attribs(
        pkg_entity='solverd_subjects',
        attributes='subjectID,fid',
        nested_columns='subjectID'
    )

    samples_dt = get_table_attribs(
        pkg_entity='solverd_samples',
        attributes='sampleID,belongsToSubject',
        nested_columns='subjectID'
    )

    experiments_data = rd3.get(
        'solverd_labinfo',
        attributes="experimentID,sampleID",
        batch_size=10000
    )

    # for row in solverdExperiments:
    for row in experiments_data:
        if 'sampleID' in row:
            if bool(row['sampleID']):
                row['sampleID'] = ','.join([
                    item['sampleID'] for item in row['sampleID']
                ])
            else:
                row['sampleID'] = None
    experiments_dt = dt.Frame(experiments_data)
    del experiments_dt['_href']

    # ///////////////////////////////////////

    # Summarise data
    print2('Summarising metadata....')
    print2('Starting with experiments and samples....')
    samples_dt.key = 'sampleID'
    summary_dt = experiments_dt[
        :, :, dt.join(samples_dt)][
            f.belongsToSubject != None,
            (f.belongsToSubject, f.sampleID, f.experimentID)][
                :, :, dt.sort(f.belongsToSubject)]

    # add missing samples
    print2('Adding samples that do not have experiment metadata (yet)....')
    summary_dt_sample_ids = summary_dt['sampleID'].to_list()[0]
    samples_dt['is_missing'] = dt.Frame([
        value not in summary_dt_sample_ids
        for value in samples_dt['sampleID'].to_list()[0]
    ])

    missing_samples_dt = samples_dt[
        (f.is_missing), (f.belongsToSubject, f.sampleID)][
            f.belongsToSubject != None, :]

    summary_dt = dt.rbind(summary_dt, missing_samples_dt, force=True)

    # merge family IDs
    print2('Merging family IDs....')
    family_dt = subjects_dt[f.fid != None, (f.subjectID, f.fid)]
    family_dt.names = {'subjectID': 'belongsToSubject'}
    family_dt.key = 'belongsToSubject'
    summary_dt = summary_dt[:, :, dt.join(family_dt)]

    # add missing subjects
    print2('Adding missing subjects that do not have samples yet....')
    summary_dt_subject_ids = summary_dt['belongsToSubject'].to_list()[0]
    subjects_dt['is_missing'] = dt.Frame([
        value not in summary_dt_subject_ids
        for value in subjects_dt['subjectID'].to_list()[0]
    ])

    missing_subjects_dt = subjects_dt[f.is_missing, :]
    missing_subjects_dt.names = {'subjectID': 'belongsToSubject'}
    summary_dt = dt.rbind(summary_dt, missing_subjects_dt, force=True)
    summary_dt = summary_dt[:, :,
                            dt.sort(f.belongsToSubject, f.sampleID, f.experimentID)]
    del summary_dt['is_missing']

    # ///////////////////////////////////////

    # create json dataset
    print2('Building tree dataset....')
    tree_dt = dt.Frame([
        {'id': '', 'belongsToSubject': '', 'fid': '', 'json': ''}])

    summary_subject_ids = dt.unique(
        summary_dt['belongsToSubject']).to_list()[0]

    for (index, subj_id) in enumerate(tqdm(summary_subject_ids)):

        # filter data for subject and init json
        subj_row = summary_dt[f.belongsToSubject == subj_id, :]
        new_subj_row = subj_row[0, (f.belongsToSubject, f.fid)]
        new_subj_row['id'] = f"RD-{index}"

        subj_json = init_json(
            _index=index,
            subject_id=subj_id,
            family_id=subj_row['fid'].to_list()[0][0]
        )

        # compile samples and experiments
        subj_sample_ids = dt.unique(subj_row['sampleID']).to_list()[0]
        if bool(subj_sample_ids) and str(subj_sample_ids) != '[None]':
            subj_json['children'] = []

            # loop through samples
            for (sample_index, sample_id) in enumerate(subj_sample_ids):
                sample_json = init_child_json(
                    _index=f"{index}.{sample_index}",
                    _id=sample_id,
                    group='sample',
                    table='samples',
                    column='sampleID'
                )

                # detect experiment IDs and loop through
                subj_expr_ids = dt.unique(
                    subj_row[f.sampleID == sample_id, f.experimentID]).to_list()[0]
                if bool(subj_expr_ids) and str(subj_expr_ids) != '[None]':
                    sample_json['children'] = []
                    for (expr_index, expr_id) in enumerate(subj_expr_ids):
                        expr_json = init_child_json(
                            _index=f"{index}.{sample_index}.{expr_index}",
                            _id=expr_id,
                            group='experiment',
                            table='labinfo',
                            column='experimentID'
                        )
                        sample_json['children'].append(expr_json)

                # add sample json object to subject level json
                subj_json['children'].append(sample_json)

            # add json to tree_data
            new_subj_row['json'] = json.dumps(subj_json)
        tree_dt = dt.rbind(tree_dt, new_subj_row, force=True)

    # ///////////////////////////////////////

    # drop first row and importy
    print2('Importing data....')
    tree_dt = tree_dt[f.id != '', :]
    tree_dt.names = {'belongsToSubject': 'subjectID'}
    rd3.import_dt('rd3stats_treedata', tree_dt)
    rd3.logout()

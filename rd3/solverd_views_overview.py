"""Solve-RD Overview Mapping
FILE: rd3_views_overview.py
AUTHOR: David Ruvolo
CREATED: 2022-05-16
MODIFIED: 2024-01-24
PURPOSE: generate dataset for rd3_overview
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""

import re
from os import environ
from dotenv import load_dotenv
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
from rd3tools.datatable import unique_values_by_id
from datatable import dt, f, as_type
load_dotenv()

print2('Connecting to RD3....')
rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


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
    data_flat = flatten_data(data_raw, nested_columns)
    return dt.Frame(data_flat)


# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Fetch RD3 Data
print2('Pulling metadata....')

subjects_dt = get_table_attribs(
    pkg_entity='solverd_subjects',
    attributes=','.join([
        'subjectID', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'solved',
        'disease', 'phenotype', 'hasNotPhenotype', 'organisation', 'ERN', 'partOfRelease'
    ]),
    nested_columns='subjectID|id|value'
)

samples_dt = get_table_attribs(
    pkg_entity='solverd_samples',
    attributes='sampleID,belongsToSubject',
    nested_columns='subjectID'
)

experiments_dt = get_table_attribs(
    pkg_entity='solverd_labinfo',
    attributes='experimentID,sampleID',
    nested_columns='sampleID'
)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Summarise data
print2('Summarising sample metadata by subject....')

subjects_dt['hasOnlyNovelOmics'] = dt.Frame([
    not bool(re.search('freeze', d))
    for d in subjects_dt['partOfRelease'].to_list()[0]
])

# get all samples by subject
samples_by_subj = unique_values_by_id(
    data=samples_dt[:, (f.belongsToSubject, f.sampleID)],
    group_by='belongsToSubject',
    column='sampleID'
)

# get number of samples by subject
sample_counts_by_subj = samples_dt[:, dt.count(), dt.by(f.belongsToSubject)]
sample_counts_by_subj.key = 'belongsToSubject'

# join samples and counts
samples_by_subj = samples_by_subj[:, :, dt.join(sample_counts_by_subj)]
samples_by_subj.names = {
    'belongsToSubject': 'subjectID',
    'sampleID': 'samples',
    'count': 'numberOfSamples'
}

# join data
subjects_dt.key = 'subjectID'
samples_by_subj.key = 'subjectID'
subjects_dt = subjects_dt[:, :, dt.join(samples_by_subj)]

# ///////////////////////////////////////

# join subject ID
print2('Summarising experiment metadata by samples....')
samples_dt.key = 'sampleID'
experiments_dt = experiments_dt[:, :, dt.join(samples_dt)]

# summarize data
expr_count_by_subj = experiments_dt[
    :, dt.count(f.experimentID), dt.by(f.belongsToSubject)]

expr_count_by_subj.names = {'experimentID': 'numberOfExperiments'}

# summarize samples by subject
expr_by_subject = unique_values_by_id(
    data=experiments_dt[:, (f.belongsToSubject, f.experimentID)],
    group_by='belongsToSubject',
    column='experimentID'
)

# join counts with experiment summary
expr_by_subject.key = 'belongsToSubject'
expr_count_by_subj.key = 'belongsToSubject'
expr_by_subject = expr_by_subject[:, :, dt.join(expr_count_by_subj)]

# join with main dataset
expr_by_subject.names = {
    'experimentID': 'experiments',
    'belongsToSubject': 'subjectID'
}

expr_by_subject.key = 'subjectID'
subjects_dt.key = 'subjectID'
subjects_dt = subjects_dt[:, :, dt.join(expr_by_subject)]

# ///////////////////////////////////////

# Add File query
# Since the files table is quite large and it takes a while to pull the data,
# it is better to make a query URL that redirects to files tables.
# print2('Getting file metadata.....')
# files_dt = get_table_attribs(
#     pkg_entity='solverd_files',
#     attributes='subjectID',
#     nested_columns='subjectID'
# )

# file_subject_ids = dt.unique(
#     files_dt[f.subjectID != None, 'subjectID']).to_list()[0]

# subjects_dt['files'] = dt.Frame([
#     f'?entity=solverd_files&hideselect=true&mod=data&filter=subjectID=={value}'
#     if value in file_subject_ids else None
#     for value in subjects_dt['subjectID'].to_list()[0]
# ])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data

print2('Renaming columns and importing....')
subjects_dt['hasOnlyNovelOmics'] = subjects_dt[
    :, as_type(f.hasOnlyNovelOmics, dt.Type.str32)]

subjects_dt['clinical_status'] = subjects_dt[
    :, as_type(f.clinical_status, dt.Type.str32)]

subjects_dt['numberOfSamples'] = subjects_dt[
    :, as_type(f.numberOfSamples, dt.Type.str32)]

subjects_dt['numberOfExperiments'] = subjects_dt[
    :, as_type(f.numberOfExperiments, dt.Type.str32)]

rd3.import_dt('solverd_overview', subjects_dt)
rd3.logout()

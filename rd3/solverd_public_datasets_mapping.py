"""Solve-RD Public Datasets Mapping
FILE: solverd_public_datasets_mapping.py
AUTHOR: David Ruvolo
CREATED: 2024-05-08
MODIFIED: 2024-05-16
PURPOSE: map public dataset identifiers into RD3
STATUS: stable; ongoing
PACKAGES: **see below**
COMMENTS: NA
"""
from os import environ
from datetime import datetime
import numpy as np
from dotenv import load_dotenv
from tqdm import tqdm
from datatable import dt, f
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data, as_key_pairs, recode_value
load_dotenv()


def unique_values_by_id(data, groupby, column, drop_duplicates=True, key_groupby=True):
    """Unique Values By Id
    For a datatable object, collapse all unique values by ID into a comma
    separated string.

    @param data datatable object
    @param groupby name of the column that will serve as the grouping variable
    @param column name of the column that contains the values to collapse
    @param drop_duplicates If True, all duplicate rows will be removed
    @param key_groupby If True, returned object will be keyed using the value named in groupby

    @param datatable object
    """
    df = data.to_pandas()
    df[column] = df.dropna(subset=[column]) \
        .groupby(groupby)[column] \
        .transform(lambda val: ','.join(set(val)))
    if drop_duplicates:
        df = df[[groupby, column]].drop_duplicates()
    output = dt.Frame(df)
    if key_groupby:
        output.key = groupby
    return output


def dt_as_recordset(data: None):
    """Datatable to recordset"""
    if 'to_pandas' not in dir(data):
        raise AttributeError('Data is not a datatable frame')
    return data.to_pandas().replace({np.nan: None}).to_dict('records')


def merge_dataset_ids(id_list, from_dt, to_dt, from_id_col, to_id_col):
    """Merge dataset IDs from one dataset to another

    :param id_list: list of identifiers that exist in the from/to datasets
    :param from_dt: the dataset to extract IDs from
    :param from_id_col: name of the column where the values in id_list exist

    :param to_dt: the dataset to copy the IDs into
    :param to_id_col: name of the column where the values in id_list exist
    """
    for _id in tqdm(id_list):
        row_dataset_ids = dt.unique(
            from_dt[f[from_id_col] == _id, 'data_ega_id']
        )
        row_dataset_id_str = ','.join(row_dataset_ids.to_list()[0])
        if row_dataset_id_str:
            to_dt[
                f[to_id_col] == _id, (f.includedInDatasets, f.should_import)
            ] = (
                row_dataset_id_str,
                True
            )


def calculate_age(dob, recent=datetime.today()):
    """Calculate age from date of birth until a recent date"""
    return recent.year - dob.year - ((recent.month, recent.day) < (dob.month, dob.day))


# ///////////////////////////////////////////////////////////////////////////////


# ~ 0 ~
# Connect to RD3 and retrieve metadata
print2('Connecting to RD3 and retrieving data....')

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# pull datasets and find unique ones
# NOTE: in the future, you will need a step here to identify new datasets
all_datasets_dt = dt.Frame(
    rd3.get('rd3_portal_novelomics_datasets', batch_size=10000)
)[:, (f.data_ega_id, f.run_or_analysis_ega_id, f.reference_type)]


datasets_dt = all_datasets_dt[
    :, dt.first(f[:]), dt.by(f.data_ega_id, f.reference_type)][
    :, (f.data_ega_id, f.reference_type)]


# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Get data required for the script

# retrieve subjects to validate incoming subjects and experiments
subjects_raw = rd3.get('solverd_subjects', batch_size=1000)
subjects = flatten_data(subjects_raw, 'subjectID|id|value')
subjects_dt = dt.Frame(subjects)
subjects_dt = subjects_dt[f.retracted != 'Y', :]

# subjectinfo_raw = rd3.get('solverd_subjectinfo', batch_size=1000)
# subjectinfo = flatten_data(subjectinfo_raw, 'subjectID|id|value')
# subjectinfo_dt = dt.Frame(subjectinfo)[:, (
#     f.subjectID,
#     f.dateOfBirth,
#     f.ageOfOnset
# )]

# if 'includedInDatasets' not in subjectinfo_dt.names:
#     subjectinfo_dt['includedInDatasets'] = ''

# CURRENT_YEAR = datetime.today().year
# subjectinfo_dt['ageAsOfToday'] = dt.Frame([
#     calculate_age(datetime(value, 7, 1))
#     if bool(value) and value <= CURRENT_YEAR else None
#     for value in subjectinfo_dt['dateOfBirth'].to_list()[0]
# ])


# retrieve samples to identify new samples
samples_raw = rd3.get('solverd_samples', batch_size=1000)
samples = flatten_data(samples_raw, 'subjectID|id|value')
samples_dt = dt.Frame(samples)


# retrieve experiments to identify new experiments
experiments_raw = rd3.get('solverd_labinfo', batch_size=1000)
experiments = flatten_data(experiments_raw, 'sampleID|id|value')
experiments_dt = dt.Frame(experiments)


# ///////////////////////////////////////

# get analysis types
releases_dt = dt.Frame(
    rd3.get('solverd_info_datareleases', attributes='id,name'))
del releases_dt['_href']

releases_dt['analysisType'] = dt.Frame([
    'WES/WGS' if 'freeze' in row[0]
    else row[1]
    for row in releases_dt[:, (f.id, f.name)].to_tuples()
])

# build reference table
dataset_analysis_dt = releases_dt.copy()[:, ('id', 'analysisType')]
dataset_analysis_dt.names = {'analysisType': 'name', 'id': 'partOfRelease'}

analysis_type_mappings = as_key_pairs(
    data=dt_as_recordset(dataset_analysis_dt),
    key_attr='partOfRelease',
    value_attr='name'
)

dataset_analysis_dt = dt.Frame(
    unique_values_by_id(
        data=dataset_analysis_dt,
        groupby='name',
        column='partOfRelease',
        key_groupby=False
    )
)

rd3.import_dt('solverd_info_datasetAnalyses', dataset_analysis_dt)

# if there are any new mappings add them here
analysis_type_mappings.update({
    'freeze1_original,freeze2_original': 'WES/WGS'
})

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Retreive incoming metadata
# using the following filters, retrieve the data in batches:
#    date_created=ge=2024-04-01T00:00:00%2B0200
#

TOTAL_ROWS = 599202
INCREMENT = 10000
portal_raw = []

for rowNumber in tqdm(range(0, TOTAL_ROWS, INCREMENT)):
    raw = rd3.get(
        'rd3_portal_release_experiments',
        q='date_created=ge=2024-04-01T00:00:00%2B0200',
        attributes='file_ega_id,project_experiment_dataset_id,sample_id,subject_id,analysis_ega_id',
        batch_size=10000,
        num=INCREMENT,
        start=rowNumber,
    )
    portal_raw.extend(raw)

portal_dt = dt.Frame(portal_raw)
del portal_dt['_href']

# rename experiment id
portal_dt.names = {'project_experiment_dataset_id': 'experiment_id'}

# join dataset ids
analysis_ega_ids = all_datasets_dt['run_or_analysis_ega_id'].to_list()[0]
portal_dt['data_ega_id'] = dt.Frame([
    all_datasets_dt[
        f.run_or_analysis_ega_id == value, 'data_ega_id'
    ].to_list()[0][0]
    if value in analysis_ega_ids else None
    for value in tqdm(portal_dt['analysis_ega_id'].to_list()[0])
])

# drop unknown cases
portal_dt = portal_dt[f.data_ega_id != None, :]

# ///////////////////////////////////////

# get unique datasets by patient, samples, and experiments
patient_dataset_dt = portal_dt[
    :, dt.first(f[:]), dt.by(f.subject_id, f.data_ega_id)][
    :, (f.subject_id, f.data_ega_id)]

sample_dataset_dt = portal_dt[
    :, dt.first(f[:]), dt.by(f.sample_id, f.data_ega_id)][
    :, (f.sample_id, f.data_ega_id)]

labinfo_dataset_dt = portal_dt[
    :, dt.first(f[:]), dt.by(f.experiment_id, f.data_ega_id)][
    :, (f.experiment_id, f.data_ega_id)]

# init import flag on main datasets
subjects_dt['should_import'] = False
samples_dt['should_import'] = False
experiments_dt['should_import'] = False
# subjectinfo_dt['should_import'] = False

# isolate ids
patient_dataset_ids = patient_dataset_dt['subject_id'].to_list()[0]
sample_dataset_ids = sample_dataset_dt['sample_id'].to_list()[0]
labinfo_dataset_ids = labinfo_dataset_dt['experiment_id'].to_list()[0]

# merge dataset ids with solve-rd dataset
print2('Merging datasets IDs with subjects....')
merge_dataset_ids(
    id_list=patient_dataset_ids,
    from_dt=patient_dataset_dt,
    from_id_col='subject_id',
    to_dt=subjects_dt,
    to_id_col='subjectID'
)

print2('Merging dataset IDs with samples....')
merge_dataset_ids(
    id_list=sample_dataset_ids,
    from_dt=sample_dataset_dt,
    to_dt=samples_dt,
    from_id_col='sample_id',
    to_id_col='sampleID'
)

print2('Merging dataset IDs with experiments....')
merge_dataset_ids(
    id_list=labinfo_dataset_ids,
    from_dt=labinfo_dataset_dt,
    to_dt=experiments_dt,
    from_id_col='experiment_id',
    to_id_col='experimentID'
)

# print2('Merging dataset IDs with subjectinfo....')
# merge_dataset_ids(
#     id_list=patient_dataset_ids,
#     from_dt=patient_dataset_dt,
#     from_id_col='subject_id',
#     to_dt=subjectinfo_dt,
#     to_id_col='subjectID'
# )

# reduce subjects to cases with datasets
new_subjects_dt = subjects_dt[f.should_import, :]
new_samples_dt = samples_dt[f.should_import, :]
new_experiments_dt = experiments_dt[f.should_import, :]
# new_subjectinfo_dt = subjectinfo_dt[f.should_import, :]

# create analysis type
new_experiments_dt['analysisType'] = dt.Frame([
    recode_value(
        mappings=analysis_type_mappings,
        value=value,
        label='analyis type mappings'
    )
    for value in new_experiments_dt['partOfRelease'].to_list()[0]
])


# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Summary datasets

# summarise subjects by dataset
for dataset in tqdm(datasets_dt['data_ega_id'].to_list()[0]):
    dataset_subjects = new_subjects_dt[
        f.includedInDatasets == dataset, :
    ]

    dataset_samples = new_samples_dt[
        f.includedInDatasets == dataset, :
    ]

    dataset_experiments = new_experiments_dt[
        f.includedInDatasets == dataset, :
    ]

    # ///////////////////////////////////////

    # get number of rows (i.e., patients)
    print2('Summarising datasets (num of rows)....')

    # set row counts
    datasets_dt[
        f.data_ega_id == dataset,
        [
            'numberOfPatients',
            'numberOfSamples',
            'numberOfExperiments'
        ]
    ] = (
        dataset_subjects.nrows,
        dataset_samples.nrows,
        dataset_experiments.nrows
    )

    # get number of males
    datasets_dt[
        f.data_ega_id == dataset,
        'numberOfMales'
    ] = dataset_subjects[f.sex1 == 'M', :].nrows

    # get number of females
    datasets_dt[
        f.data_ega_id == dataset,
        'numberOfFemales'
    ] = dataset_subjects[f.sex1 == 'F', :].nrows

    # set analysis types
    datasets_dt[
        f.data_ega_id == dataset,
        'analysisTypes'
    ] = ','.join(dt.unique(dataset_experiments['analysisType']).to_list()[0])

    # set erns
    dataset_erns = []
    for ern_str in dt.unique(dataset_subjects['ERN']).to_list()[0]:
        erns = ern_str.split(',')
        for ern in erns:
            if ern not in dataset_erns:
                dataset_erns.append(ern)
    dataset_erns.sort()

    datasets_dt[
        f.data_ega_id == dataset, 'ERN'
    ] = ','.join(dataset_erns)

    # ///////////////////////////////////////

    # get unique disease codes
    print2('Finding distinct ORDO codes....')

    dataset_subject_diseases = dt.unique(
        dataset_subjects[f.disease != None, 'disease']).to_list()[0]

    dataset_disease_codes = []
    for code in dataset_subject_diseases:
        if ',' in code:
            for subcode in code.split(','):
                dataset_disease_codes.append(subcode)
        else:
            dataset_disease_codes.append(code)

    # add to datasets table
    dataset_disease_codes_set = list(set(dataset_disease_codes))
    dataset_disease_codes_set.sort()
    datasets_dt[
        f.data_ega_id == dataset, 'ordoCodes'
    ] = ','.join(dataset_disease_codes_set)

    # ///////////////////////////////////////

    # get unique HPO
    print2('Finding distinct HPO codes....')

    dataset_subject_hpo = dt.unique(
        dataset_subjects[f.phenotype != None, 'phenotype']
    ).to_list()[0]

    dataset_hpo_codes = []
    for code in dataset_subject_hpo:
        if ',' in code:
            for subcode in code.split(','):
                dataset_hpo_codes.append(subcode)
        else:
            dataset_hpo_codes.append(code)

    dataset_hpo_codes_set = list(set(dataset_hpo_codes))
    dataset_hpo_codes_set.sort()
    datasets_dt[
        f.data_ega_id == dataset, 'hpoCodes'
    ] = ','.join(dataset_hpo_codes_set)


# ///////////////////////////////////////

# ~ 4 ~
# import datasets

datasets_dt.names = {
    'data_ega_id': 'id',
    'reference_type': 'datasetType'
}

rd3.import_dt('solverd_info_datasets', datasets_dt)
rd3.import_dt('solverd_subjects', new_subjects_dt)
rd3.import_dt('solverd_samples', new_samples_dt)
rd3.import_dt('solverd_labinfo', new_experiments_dt)

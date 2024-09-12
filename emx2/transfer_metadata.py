"""Transfer data from EMX1 to EMX2
FILE: transfer_metadata.py
AUTHOR: David Ruvolo
CREATED: 2023-05-09
MODIFIED: 2023-05-11
PURPOSE: import files and data into emx2 instance
STATUS: stable
PACKAGES: **See below**
COMMENTS: NA
"""

from os import environ, system
from datatable import dt, f, as_type
from dotenv import load_dotenv
from molgenis_emx2_pyclient.client import Client
from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import flattenDataset, recodeValue
from emx2.utils import to_csv, recode_comma_string
load_dotenv()


def clear():
    """Clear terminal"""
    system("clear")


rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# emx2 = EMX2(environ['MOLGENIS_EMX2_HOST'])
# emx2.signin(environ['MOLGENIS_EMX2_USR'], environ['MOLGENIS_EMX2_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull RD3 metadata

# create a subset of RD3 using the familyID of subjects with 3 or more samples
ids_dt = dt.Frame(
    rd3.get(
        'solverd_overview',
        q='numberOfSamples=ge=3',
        attributes='subjectID,fid',
        num=10
    )
)

del ids_dt['_href']


# create queries to pull subject metadata
FID_QUERY = ','.join([f"fid=q={id}" for id in ids_dt['fid'].to_list()[0]])
PID_QUERY = ','.join([
    f'subjectID=q={id}' for id in ids_dt['subjectID'].to_list()[0]
])

SAMPLE_QUERY = ','.join([
    f"belongsToSubject=q={id}" for id in ids_dt['subjectID'].to_list()[0]
])

# ///////////////////////////////////////

# retrieve data from subjects and subject info
subjects_raw = rd3.get('solverd_subjects', q=f"({FID_QUERY})")
subjects_dt = dt.Frame(flattenDataset(subjects_raw, 'subjectID|id|value'))

subjectinfo_raw = rd3.get('solverd_subjectinfo', q=f"({PID_QUERY})")
subjectinfo_dt = dt.Frame(
    flattenDataset(
        subjectinfo_raw,
        'subjectID|id|value'
    )
)

subjectinfo_dt['dateOfBirth'] = subjectinfo_dt[
    :, as_type(f.dateOfBirth, dt.Type.str32)]


# pull sample metadata
samples_raw = rd3.get('solverd_samples', q=SAMPLE_QUERY)
samples_dt = dt.Frame(flattenDataset(samples_raw, 'subjectID|id|value'))


# pull experiment metadata
LAB_QUERY = ','.join([
    f"sampleID=q={id}" for id in samples_dt['sampleID'].to_list()[0]
])

labinfo_raw = rd3.get('solverd_labinfo', q=LAB_QUERY)
labinfo_dt = dt.Frame(flattenDataset(labinfo_raw, 'sampleID|id'))


# Pull file metadata
files_raw = rd3.get('solverd_files', q=PID_QUERY)
files_dt = dt.Frame(flattenDataset(
    files_raw, 'subjectID|sampleID|experimentID|id'))

# check for missing file identifiers
files_dt['missingExperiment'] = dt.Frame([
    id not in labinfo_dt['experimentID'].to_list()[0]
    for id in files_dt['experimentID'].to_list()[0]
])

files_dt['missingSample'] = dt.Frame([
    id not in samples_dt['sampleID'].to_list()[0]
    for id in files_dt['sampleID'].to_list()[0]
])

# drop rows where sample was not included in the export
# files_dt[:, dt.count(), dt.by(f.missingSample)]
# files_dt[:, dt.count(), dt.by(f.missingExperiment)]
files_dt = files_dt[f.missingSample != True, :]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Transform data into FDH template


# ~ 2c ~
# Recode ERNs
ernMappings = {
    'ern_euro_nmd': 'ERN EURO-NMD',
    'ern_rnd': 'ERN-RND',
}

subjects_dt['ERN'] = dt.Frame([
    recodeValue(mappings=ernMappings, value=value, label='ERN')
    if value else value
    for value in subjects_dt['ERN'].to_list()[0]
])

samples_dt['ERN'] = dt.Frame([
    recodeValue(mappings=ernMappings, value=value, label='ERN')
    if value else value
    for value in samples_dt['ERN'].to_list()[0]
])

# overviewDT['ERN'] = dt.Frame([
#     recodeValue(mappings=ernMappings, value=value, label='ERN')
#     if value else value
#     for value in overviewDT['ERN'].to_list()[0]
# ])


# ~ 2d ~
# Recode phenotypes
hpo = emx2.query(
    database='RD3',
    query="""{
    Phenotype {
      name
      code
    }
  }
  """
).json().get('data', {}).get('Phenotype')

hpoMappings = {}
for entry in hpo:
    hpoMappings[entry['code']] = entry['name']


subjects_dt['phenotype'] = dt.Frame([
    recode_comma_string(hpoMappings, value) if value else value
    for value in subjects_dt['phenotype'].to_list()[0]
])

subjects_dt['hasNotPhenotype'] = dt.Frame([
    recode_comma_string(hpoMappings, value) if value else value
    for value in subjects_dt['hasNotPhenotype'].to_list()[0]
])

if 'ageOfOnset' in subjectinfo_dt.names:
    subjectinfo_dt['ageOfOnset'] = dt.Frame([
        recode_comma_string(hpoMappings, value) if value else value
        for value in subjectinfo_dt['ageOfOnset'].to_list()[0]
    ])


# overviewDT['phenotype'] = dt.Frame([
#     recode_comma_string(hpoMappings, value) if value else value
#     for value in overviewDT['phenotype'].to_list()[0]
# ])

# overviewDT['hasNotPhenotype'] = dt.Frame([
#     recode_comma_string(hpoMappings, value) if value else value
#     for value in overviewDT['hasNotPhenotype'].to_list()[0]
# ])

# ~ 2e ~
# Recode Diseases
# disease = emx2.query(
#     database='RD3',
#     query="""{
#     Disease {
#       name
#       code
#     }
#   }
#   """
# ).json().get('data', {}).get('Disease')

# diseaseMappings = {}
# for entry in disease:
#     diseaseMappings[entry['code']] = entry['name']


# subjects_dt['disease'] = dt.Frame([
#     recode_comma_string(diseaseMappings, value) if value else value
#     for value in subjects_dt['disease'].to_list()[0]
# ])

# overviewDT['disease'] = dt.Frame([
#     recode_comma_string(diseaseMappings, value) if value else value
#     for value in overviewDT['disease'].to_list()[0]
# ])

# ///////////////////////////////////////

# ~ 3 ~
# Save data

# update names
# subjects_dt.names = {
#     'fid': 'familyID',
#     'pid': 'paternalID',
#     'mid': 'maternalID'
# }

# # save
# to_csv('emx2/data/subjects.csv', subjects_dt)
# to_csv('emx2/data/subjectinfo.csv', subjectinfo_dt)
# to_csv('emx2/data/samples.csv', samples_dt)
# to_csv('emx2/data/labinfo.csv', labinfoDT)
# to_csv('emx2/data/files.csv', filesDT)
# to_csv('emx2/data/overview.csv', overviewDT)

# rd3.logout()

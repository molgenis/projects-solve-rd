"""Build GDI Datasets
FILE: gdi_build_datasets.py
AUTHOR: David Ruvolo
CREATED: 2023-05-04
MODIFIED: 2024-02-20
PURPOSE: read raw data and prepare datasets for EMX2 instance
STATUS: stable
PACKAGES: **see below**
COMMENTS: For information on the structure of PED files, see the following:
https://gatk.broadinstitute.org/hc/en-us/articles/360035531972-PED-Pedigree-format
"""

import ast
import csv
import re
from os import environ
from datetime import datetime
from datatable import dt, f, fread, as_type
from dotenv import load_dotenv
from molgenis_emx2_pyclient.client import Client
load_dotenv()


def to_csv_string(data):
    """Convert a datatable object to a text/csv string.
    :param: data datatable object
    :return: text/csv string
    """
    return data.to_pandas().to_csv(index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')


# connect to test instance
db = Client(
    url=environ['MOLGENIS_HOST'],
    schema='PortalGDI', token=environ['MOLGENIS_TOKEN']
)

# db.signin(environ['MOLGENIS_USR'], environ['MOLGENIS_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Load data and define mapping objects

# load datasets
checksumsDT = dt.rbind(
    fread('data/gdi_checksums.csv'),
    fread('data/md5sums_current_files.txt')[:, {'name': f[1], 'md5': f[0]}],
    force=True
)

filesDT = fread('data/gdi_files.csv')
pedDT = fread('data/gdi_ped.csv')
phenopacketDT = fread('data/gdi_phenopacket.csv')

# define mapping objects
genderAtBirth = {
    1: 'assigned male at birth',
    2: 'assigned female at birth',
    'male': 'assigned male at birth',
    'female': 'assigned female at birth',
    'other': 'assigned other at birth'
}

affectedStatuses = {
    -9: 'missing',
    0: 'missing',
    1: 'unaffected',
    2: 'affected'
}

fileFormats = {
    'ped': 'PED',
    'vcf': 'VCF',
    'phenopacket': 'JSON',
    'fastq': 'FASTQ',
}

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare the PED dataset


# remove 0s from maternal- and paternal identifiers
pedDT['maternal_id'] = dt.Frame([
    None if value == '0' else value
    for value in pedDT['maternal_id'].to_list()[0]
])

pedDT['paternal_id'] = dt.Frame([
    None if value == '0' else value
    for value in pedDT['paternal_id'].to_list()[0]
])

# identify the index
pedDT['is_index'] = dt.Frame([
    all(row)
    for row in pedDT[:, (f.subject_id, f.maternal_id, f.paternal_id)].to_tuples()
])

# pedDT = pedDT[f.is_index,:]

# recode sex to FDP terminology
pedDT['sex'] = dt.Frame([
    genderAtBirth[value] if value in genderAtBirth else value
    for value in pedDT['sex'].to_list()[0]
])

# recode affected status
pedDT['affected_status'] = dt.Frame([
    affectedStatuses[value] if value in affectedStatuses else value
    for value in pedDT['affected_status'].to_list()[0]
])

# extract case
pedDT['case'] = dt.Frame([
    re.search(r'case[0-9]', value.lower()).group()
    for value in pedDT['file'].to_list()[0]
])


# create index patients
idsByCase = pedDT[:, (f.subject_id, f.case, f.is_index)]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Prepare Phenopackets Dataset

# ~ 2a ~
# Extract Metadata
# parse json strings and extract metadata accordingly

# set case
phenopacketDT['case'] = dt.Frame([
    re.search(r'case[0-9]', value.lower()).group()
    for value in phenopacketDT['name'].to_list()[0]
])

# extract primary information from phenopacket.subject
phenopacketDT[['subject_id', 'dateOfBirth', 'sex']] = dt.Frame([
    (
        ast.literal_eval(value)['id'],
        ast.literal_eval(value)['subject']['dateOfBirth'],
        ast.literal_eval(value)['subject']['sex'],
    )
    if value else None
    for value in phenopacketDT['phenopacket'].to_list()[0]
])

# identify index patient
phenopacketDT['is_index'] = dt.Frame([
    value in pedDT[f.is_index, 'subject_id'].to_list()[0]
    for value in phenopacketDT['subject_id'].to_list()[0]
])
# phenopacketDT = phenopacketDT[f.keep, :]

# collapse disease codes
phenopacketDT['diseases'] = dt.Frame([
    ','.join([
        # entry['term']['id']
        entry['term']['label']
        for entry in ast.literal_eval(value)['diseases']
    ])
    if bool(ast.literal_eval(value)['diseases']) else None
    for value in phenopacketDT['phenopacket'].to_list()[0]
])

# recode disease terms
phenopacketDT['diseases'] = dt.Frame([
    None if not value else (
        re.sub(
            'CENTRAL CORE DISEASE OF MUSCLE',
            'Central core disease',
            value
        )
        if 'CENTRAL CORE DISEASE OF MUSCLE' in value
        else value
    )
    for value in phenopacketDT['diseases'].to_list()[0]
])

# collate observed HPO codes
phenopacketDT['observedHpo'] = dt.Frame([
    ','.join([
        # entry['type']['id']
        entry['type']['label']
        for entry in ast.literal_eval(value)['phenotypicFeatures']
        if not entry.get('negated')
    ])
    if bool(ast.literal_eval(value)['phenotypicFeatures']) else None
    for value in phenopacketDT['phenopacket'].to_list()[0]
])


# collate unobserved HPO terms
phenopacketDT['unobservedHpo'] = dt.Frame([
    ','.join([
        # entry['type']['id']
        entry['type']['label']
        for entry in ast.literal_eval(value)['phenotypicFeatures']
        if entry.get('negated')
    ])
    if bool(ast.literal_eval(value)['phenotypicFeatures']) else None
    for value in phenopacketDT['phenopacket'].to_list()[0]
])


# extract solved status
phenopacketDT['solvedStatus'] = dt.Frame([
    ast.literal_eval(value)['resolutionStatus']
    if 'resolutionStatus' in ast.literal_eval(value).keys() else None
    for value in phenopacketDT['interpretation'].to_list()[0]
])

# drop json cols
del phenopacketDT[:, ['phenopacket', 'interpretation']]

phenopacketDT = phenopacketDT[:, :, dt.sort(f.subject_id)]

# ///////////////////////////////////////

# ~ 2b ~
# Recode data

# format date of birth
phenopacketDT['dateOfBirth'] = dt.Frame([
    None if 'unknown' in value else value.split('T00:')[0]
    for value in phenopacketDT['dateOfBirth'].to_list()[0]
])

# calcuate age
phenopacketDT['today'] = datetime.today().strftime('%Y-%m-%d')
phenopacketDT['dateOfBirth'] = as_type(f.dateOfBirth, dt.Type.date32)
phenopacketDT['today'] = as_type(f.today, dt.Type.date32)

phenopacketDT['age'] = dt.Frame([
    round(int((row[1] - row[0]).days) / 364.25, 4)
    if all(row) else None
    for row in phenopacketDT[:, ['dateOfBirth', 'today']].to_tuples()
])

del phenopacketDT['today']

# recode 'sexAtBirth'
phenopacketDT['sex'] = dt.Frame([
    genderAtBirth[value.lower()]
    for value in phenopacketDT['sex'].to_list()[0]
])


# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Prepare filesDT


# merge checksum with file metadata
md5 = checksumsDT.copy()[:, {'id': f.name, 'md5checksum': f.md5}]
md5['name'] = dt.Frame([
    value.replace('.md5', '') if value else value
    for value in md5['id'].to_list()[0]
])

md5.key = 'name'
filesDT.key = 'name'

filesDT = filesDT[:, :, dt.join(md5)]

# extract case
filesDT['case'] = dt.Frame([
    re.search(r'case[0-9]', value.lower()).group()
    for value in filesDT['name'].to_list()[0]
])

# create (file)format
filesDT['format'] = dt.Frame([
    re.search(r'fastq|phenopacket|ped|vcf', value).group()
    for value in filesDT['name'].to_list()[0]
])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Create FDP datasets

# ~ 4a ~
# create individuals
individuals = dt.Frame(
    id=dt.unique(
        dt.rbind(
            pedDT['subject_id'],
            phenopacketDT['subject_id']
        )
    )
)

# copy over phenopacket metadata
phenoData = phenopacketDT[:, (f.subject_id, f.age, f.sex)]

phenoData.names = {'subject_id': 'id'}
phenoData.key = 'id'
individuals.key = 'id'

individuals = individuals[:, :, dt.join(phenoData)]

# copy over missing subject metadata from PED dataset
individuals['sex'] = dt.Frame([
    pedDT[f.subject_id == row[0], 'sex'].to_list()[0][0]
    if (not bool(row[1])) and (row[0] in pedDT['subject_id'].to_list()[0])
    else row[1]
    for row in individuals[:, ['id', 'sex']].to_tuples()
])

# ///////////////////////////////////////

# ~ 4b ~
# Create Individual Diseases

# transform diseases from wide (comma separated string) to long (i.e., rows)
diseaseDF = phenopacketDT[f.diseases != None,
                          (f.subject_id, f.diseases)].to_pandas()
diseaseDF['diseases'] = diseaseDF['diseases'].apply(
    lambda value: value.split(','))
diseaseDF = diseaseDF.explode('diseases').reset_index(drop=True)
diseaseDF['id'] = diseaseDF['subject_id'] \
    + '-' + diseaseDF.groupby('subject_id') \
    .cumcount() \
    .astype(str)

individualDiseases = dt.Frame(diseaseDF)
del diseaseDF

# compile disease record IDs and add to individuals
individuals['diseases'] = dt.Frame([
    ','.join(individualDiseases[f.subject_id == value, 'id'].to_list()[0])
    if value in individualDiseases['subject_id'].to_list()[0] else None
    for value in individuals['id'].to_list()[0]
])

individualDiseases = individualDiseases[:, (f.id, f.diseases)]

# ///////////////////////////////////////

# ~ 4c ~
# Prepare data for individual phenotypic features

hpoDF = phenopacketDT[f.observedHpo != None,
                      (f.subject_id, f.observedHpo)].to_pandas()
hpoDF['observedHpo'] = hpoDF['observedHpo'].apply(
    lambda value: value.split(','))
hpoDF = hpoDF.explode('observedHpo').reset_index(drop=True)
hpoDF['id'] = hpoDF['subject_id'] \
    + '-' + hpoDF.groupby('subject_id') \
    .cumcount() \
    .astype(str)

individualPhenotypicFeatures = dt.Frame(hpoDF)
del hpoDF

# compile hpo records and add to individuals
individuals['phenotypicFeatures'] = dt.Frame([
    ','.join(
        individualPhenotypicFeatures[f.subject_id == value, 'id'].to_list()[0])
    if value in individualPhenotypicFeatures['subject_id'].to_list()[0]
    else None
    for value in individuals['id'].to_list()[0]
])

individualPhenotypicFeatures = individualPhenotypicFeatures[:, {
    'id': f.id,
    'featureType': f.observedHpo
}]

# ///////////////////////////////////////

# ~ 4d ~
# Create data for files tables

files = filesDT.copy()
del files[:, (f.inode, f.extension, f.size, f.id)]

files['individuals'] = dt.Frame([
    None if row[1] not in dt.unique(idsByCase['case']).to_list()[0] else (
        ','.join(idsByCase[f.case == row[1], 'subject_id'].to_list()[0])
        if row[0] == 'ped'
        else idsByCase[(f.is_index) & (f.case == row[1]), 'subject_id'].to_list()[0][0]
    )
    for row in files[:, ['format', 'case']].to_tuples()
])


# recode file formats
files['format'] = dt.Frame([
    fileFormats[value] if value else value
    for value in files['format'].to_list()[0]
])

# subsitute md5checksum where missing with 0
files['md5checksum'] = dt.Frame([
    '-'.join(list(filter(None, [row[1], row[2]]))).replace(',', '_')
    if not bool(row[0]) else row[0]
    for row in files[:, ['md5checksum', 'individuals', 'name']].to_tuples()
])

files['identifier'] = files['name']

files['server'] = 'Gearshift'
del files['case']

# ///////////////////////////////////////////////////////////////////////////////

# ~ 5 ~
# Import
# Import data in reverse order due to references

individualDiseases[:, {'id': f.id, 'diseaseCode': f.diseases}] \
    .to_csv("local_portal/IndividualsDiseases.csv")

individualPhenotypicFeatures \
    .to_csv("local_portal/IndividualsPhenotypicFeatures.csv")

individuals[:, {
    'id': f.id,
    'sex': f.sex,
    'age_age_iso8601duration': f.age,
    'diseases': f.diseases,
    'phenotypicFeatures': f.phenotypicFeatures
}].to_csv("local_portal/Individuals.csv")

files.to_csv('local_portal/Files.csv')

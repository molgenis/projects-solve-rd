#//////////////////////////////////////////////////////////////////////////////
# FILE: rd3_data_phenopacket_extraction.py
# AUTHOR: David Ruvolo
# CREATED: 2022-08-01
# MODIFIED: 2022-09-06
# PURPOSE: Read and extract data from Phenopacket files
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: The purpose of this script is to locate all available phenopacket
# files on the cluster, extract and process the contents of the files, and
# import the data into a portal table: rd3_portal_cluster_phenopacket. Once
# this script is run, run the processing script `rd3_data_phenopacket_processing`.
# This script is also designed to run and import directly into RD3 ACC and PROD.
# All validation should take place in the processing script.
#
# Once this script has completed, you will need to do a few things.
#   1. Validate all codes in `unknownHpoCodes`: make sure all codes exist in RD3
#      or map to an updated value. There should only be a few cases so they
#      can be manually reviewed.
#
#   2. Validated all codes in `unknownDiseaseCodes`: check all codes to make sure
#      they are valid or if they were mapped to a new code. Use https://www.omim.org/
#       to search for codes.
#
# To view the EMX, see the file model/rd3portal_cluster.yaml
#//////////////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
from datatable import dt
from os import environ
from tqdm import tqdm
import re
import sys

load_dotenv()
sys.path.append(environ['SYS_PATH'])

from rd3.api.molgenis2 import Molgenis
from rd3.utils.clustertools import clustertools
from rd3.utils.phenopacketTools import (
  recodeSexCodes,
  formatDateOfBirth,
  unpackDiseaseCodes,
  unpackPhenotypicFeatures
)


# set latest phenopacket release
currentRelease = 'freeze3'

# load excluded files dataset
with open('data/files_phenopackets_corrupt.txt', 'r') as file:
  excludedFiles = [line.replace('\n', '') for line in file.readlines()]
  file.close()  
del file

# connect to RD3
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# Disease Code Mappings
# To add a new mapping, use the following format:
# 'INCORRECT_CODE' : 'NEW_CODE'
diseaseCodeMappings = {
  'MIM_159000': 'MIM_609200',
  'MIM_159001': 'MIM_181350',
  'MIM_607569': 'MIM_603689',
  'ORDO_856': '',  # no known mapping
  'ORDO_ 104010': 'ORDO_104010'
}

# /////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Start Molgenis Session and Pull Required Data
#
# In order to process new phenopacket files, it is important to compare values
# with new exiting RD3 metadata. This allows us to import the values that have
# changed rather than everything. Once the contents of the files have been
# processed and evaluated, we can import them into RD3. These values will be
# imported into the `subject` and `subjectinfo` tables. The attributes that
# are managed by this script are listed in the GET requests below.

# rd3._print('Compiling a list of existing subject identifiers....')

# ~ 1a ~
# Compile subject metadata
# pull subject metadata for the current freeze
# freeze = rd3.get(entity='rd3_overview',attributes='subjectID,patch', batch_size=10000)

# isolate subjectIDs and patch
# rd3._print('Extracting patch information and creating a list of subject IDs....')

# subjects = []
# for row in tqdm(freeze):
#   subject = {'subjectID': row['subjectID'], 'release': []}
#   if isinstance(row['patch'], list):
#     subject['release'] = [item['id'] for item in row['patch'] if 'patch' not in item['id']]

#   if isinstance(row['patch'], dict):
#     subject['release'] = row['patch']['id']

#   subject['release'] = ','.join(subject['release']) if subject['release'] else None
#   subjects.append(subject)

# # get list of IDs only
# subjectIDs = [row['subjectID'] for row in subjects]

# ~ 1b ~
# Create Reference datasets
rd3._print('Fetching reference datasets....')
hpo_codes_raw = rd3.get(entity='rd3_phenotype', batch_size=10000)
disease_codes_raw = rd3.get(entity='rd3_disease', batch_size=10000)
hpo_codes = [row['id'] for row in hpo_codes_raw]
disease_codes = [row['id'] for row in disease_codes_raw]

del hpo_codes_raw, disease_codes_raw

# //////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Read and process phenopacket data
#
# Compile a list of all available Phenopacket files on the cluster and process
# each file individually. The processing step evaluates the contents of all
# nested objects in the json file, specifically we are interested in the
# 'phenopacket' property. In this property, we can extract metadata on the
# subject, phenotypicFeatures, and diseases.
#
# Values aren't compared to existing data in RD3 as files for all patients
# are regenerated. Therefore, we can processing and import the data directly.
# All invalid or unknown HPO-, disease-, and onset codes are flagged for
# manual review. These codes will either need to be added to the appropriate
# lookup table or need to be mapped to a new value. These cases should be
# reconciled before importing into RD3.

# create list of all available JSON files
basePath = f"{environ['CLUSTER_BASE']}/{currentRelease}/phenopackets"
allFiles = clustertools.listFiles(path=basePath)

phenopacketFiles = [
  file for file in allFiles
  if (re.search(r'(.json)$', file['filename'])) and (file['filename'] not in excludedFiles)
]

rd3._print('Found', len(phenopacketFiles), 'phenopacket files')
rd3._print('Starting file processing...')
phenopackets = []

for file in tqdm(phenopacketFiles):
  json = clustertools.readJson(path=file['filepath'])
  phenopacket = json['phenopacket']
  result = {
    'phenopacketsID': file['filename'],
    'clusterRelease': basePath,
    'subjectID': phenopacket['id'],
    'dateofBirth': formatDateOfBirth(phenopacket.get('subject').get('dateOfBirth')),
    'sex1': recodeSexCodes(phenopacket.get('subject').get('sex')),
    'phenotype': None,
    'hasNotPhenotype': None,
    'disease': None,
    'ageOfOnset': None,
    'subjectExists': None,
    'releasesWhereSubjectExists': None,
    'unknownOnsetCodes': None,
    'unknownHpoCodes': None,
    'unknownDiseaseCodes': None,
  }

  # ///////////////////////////////////////

  # ~ 2 ~
  # make sure 'phenotypicFeatures' exists and triage HPO codes into
  # 'has', 'has not', and 'unknown'. All unknown codes will need to be manually
  # verified before importing into RD3. Unknown codes will remain in the HPO columns.
  if bool(phenopacket.get('phenotypicFeatures')):
    patientHpoCodes = unpackPhenotypicFeatures(data=phenopacket['phenotypicFeatures'])
    patientHpoUnknown = []

    # ObservedPhenotypes isolate unknown cases
    if patientHpoCodes['phenotype']:
      for code in patientHpoCodes['phenotype']:
        if code not in hpo_codes:
          patientHpoUnknown.append(code)

    # Unobserved Phenotypes isolate unknown cases
    if patientHpoCodes['hasNotPhenotype']:
      for code in patientHpoCodes['hasNotPhenotype']:
        if code not in hpo_codes:
          patientHpoUnknown.append(code)

    # collapse codes
    result['phenotype'] = ','.join(patientHpoCodes['phenotype']) if patientHpoCodes['phenotype'] else None
    result['hasNotPhenotype'] = ','.join(patientHpoCodes['hasNotPhenotype']) if patientHpoCodes['hasNotPhenotype'] else None
    result['unknownHpoCodes'] = ','.join(patientHpoUnknown) if patientHpoUnknown else None

  # ///////////////////////////////////////

  # ~ 3 ~
  # make sure `diseases` exist first
  if bool(phenopacket.get('diseases')):
    diseases = unpackDiseaseCodes(data=phenopacket['diseases'], mappings=diseaseCodeMappings)
    patientDiseasesUnknown = []

    # triage dx IDs isolate invalid codes for review
    for code in diseases['diagnostic']:
      if (code not in disease_codes) and (code != ''):
          patientDiseasesUnknown.append(code)

    result['disease'] = ','.join(diseases['diagnostic']) if diseases['diagnostic'] else None
    result['unknownDiseaseCodes'] = ','.join(patientDiseasesUnknown) if patientDiseasesUnknown else None

    # triage onset IDs isolate invalid codes for review
    if len(diseases['onset']) > 0:
      onset_codes_unknown = []
      for code in diseases['onset']:
        if code not in hpo_codes:
          onset_codes_unknown.append(code)

      result['ageOfOnset'] = ','.join(diseases['onset']) if diseases['onset'] else None
      result['unknownOnsetCodes'] = ','.join(onset_codes_unknown) if onset_codes_unknown else None
  
  # bind patient record to main object
  phenopackets.append(result)

# //////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data
rd3._print('Preparing data for import....')
rd3.delete('rd3_portal_cluster_phenopacket')
phenopacketsDT = dt.Frame(phenopackets)

# change data type void to string32
rd3._print('Checking data for void columns....')
for column in phenopacketsDT.names:
  if phenopacketsDT[column].types[0] == dt.Type.void:
    rd3._print('Changing class for column:', column)
    phenopacketsDT[column] = phenopacketsDT[column][:, dt.as_type(dt.f[column], dt.str32)]

rd3._print('Importing dataset....')
rd3.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = phenopacketsDT
)

rd3_prod.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = phenopacketsDT
)

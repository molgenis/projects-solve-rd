# '////////////////////////////////////////////////////////////////////////////
# ' FILE: rd3_data_phenopacket_extraction.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2022-08-01
# ' MODIFIED: 2022-08-04
# ' PURPOSE: Read and extract data from Phenopacket files
# ' STATUS: stable
# ' PACKAGES: **see below**
# ' COMMENTS: see model/rd3portal_cluster.yaml
# '////////////////////////////////////////////////////////////////////////////

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
currentRelease = 'freeze2'

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


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

rd3._print('Compiling a list of existing subject identifiers....')


# ~ 1a ~
# Compile subject metadata
# pull subject metadata for the current freeze
freeze = rd3.get(entity='rd3_overview',attributes='subjectID,patch', batch_size=10000)

# isolate subjectIDs and patch
rd3._print('Extracting patch information and creating a list of subject IDs....')

subjects = []
for row in tqdm(freeze):
  subject = {'subjectID': row['subjectID'], 'release': []}
  if isinstance(row['patch'], list):
    subject['release'] = [item['id'] for item in row['patch'] if 'patch' not in item['id']]

  if isinstance(row['patch'], dict):
    subject['release'] = row['patch']['id']

  subject['release'] = ','.join(subject['release']) if subject['release'] else None
  subjects.append(subject)


# get list of IDs only
subjectIDs = [row['subjectID'] for row in subjects]


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
#
# Using the object `shouldProcess`, you can process certain data elements
# in the phenopacket files. This may be useful for if you need to refresh
# only disease codes or disease codes.


# create list of all available JSON files
basePath = f"{environ['CLUSTER_BASE']}/{currentRelease}/phenopacket"
allFiles = clustertools.listFiles(path=basePath)

phenopacketFiles = [file for file in allFiles if re.search(r'(.json)$', file['filename'])]
rd3._print('Found', len(phenopacketFiles), 'phenopacket files')

# init loop params and objects
rd3._print('Starting file processing...')
phenopackets = []

for index,file in enumerate(phenopacketFiles):
  rd3._print('Processing',file['filename'],'(',index,'of',len(phenopacketFiles),')')

  rd3._print('Reading file....')
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
  # verified before importing into RD3.
  rd3._print('Extracting and recoding phenotypic features data...')
  if bool(phenopacket.get('phenotypicFeatures')):
    patientHpoCodes = unpackPhenotypicFeatures(data=phenopacket['phenotypicFeatures'])
    patient_hpo_has = []
    patient_hpo_hasnot = []
    patient_hpo_unknown = []

    # ObservedPhenotypes validate codes and isolate unknown cases
    rd3._print('Processing observed phenotypes....')
    if patientHpoCodes['phenotype']:
      for code in patientHpoCodes['phenotype']:
        if code in hpo_codes:
          patient_hpo_has.append(code)
        else:
          rd3._print('Unknown HPO code -', str(code))
          patient_hpo_unknown.append(result['id'])

    # Unobserved Phenotypes validate codes and isolate unknown cases
    rd3._print('Processing unobserved phenotypes....')
    if patientHpoCodes['hasNotPhenotype']:
      for code in patientHpoCodes['hasNotPhenotype']:
        if code in hpo_codes:
          patient_hpo_hasnot.append(code)
        else:
          rd3._print('Unknown HPO code', str(code))
          patient_hpo_unknown.append(result['id'])

    # collapse codes
    result['phenotype'] = ','.join(patient_hpo_has) if patient_hpo_has else None
    result['hasNotPhenotype'] = ','.join(patient_hpo_hasnot) if patient_hpo_hasnot else None
    result['unknownHpoCodes'] = ','.join(patient_hpo_unknown) if patient_hpo_unknown else None

  # ///////////////////////////////////////

  # ~ 3 ~
  # make sure `diseases` exist first
  rd3._print('Extracting and processing disease codes....')
  if bool(phenopacket.get('diseases')):
    diseases = unpackDiseaseCodes(data=phenopacket['diseases'], mappings=diseaseCodeMappings)
    patient_diseases_has = []
    patient_diseases_unknown = []

    # triage dx IDs isolate invalid codes for review
    rd3._print('Validating codes....')
    for code in diseases['diagnostic']:
      if code in disease_codes:
        patient_diseases_has.append(code)
      else:
        if code != '':
          rd3._print('Unknown disease code - ', str(code))
          patient_diseases_unknown.append(code)
          diseases['dx'].remove(code)

    result['disease'] = ','.join(patient_diseases_has) if patient_diseases_has else None
    result['unknownDiseaseCodes'] = ','.join(patient_diseases_unknown) if patient_diseases_unknown else None

    # triage onset IDs isolate invalid codes for review
    if len(diseases['onset']) > 0:
      rd3._print('Processing onset codes....')
      onset_codes_known = []
      onset_codes_unknown = []

      for code in diseases['onset']:
        if code in hpo_codes:
          onset_codes_known.append(code)
        else:
          rd3._print('Unknown onset code:', str(code))
          onset_codes_unknown.append(code)

      result['ageOfOnset'] = ','.join(onset_codes_known) if onset_codes_known else None
      result['unknownOnsetCodes'] = ','.join(onset_codes_unknown) if onset_codes_unknown else None

  # check to see if subject exists
  rd3._print('Setting status variables....')
  result['subjectExists'] = result['subjectID'] in subjectIDs
  if result['subjectExists']:
    result['releasesWhereSubjectExists'] = ','.join([
      row['release'] for row in subjects if row['subjectID'] == result['subjectID']
    ])
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

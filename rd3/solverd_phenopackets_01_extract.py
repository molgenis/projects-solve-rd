#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_cluster_phenopackets_01_extract.py
# AUTHOR: David Ruvolo
# CREATED: 2023-01-23
# MODIFIED: 2023-01-23
# PURPOSE: extract phenopacket information from a specific location
# STATUS: in.progress
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
from datatable import dt, fread
from os import environ, listdir, path
from tqdm import tqdm
import re
import sys
import json

load_dotenv()
sys.path.append(environ['SYS_PATH'])

from rd3.api.molgenis2 import Molgenis
from rd3.utils.phenopacketTools import (
  recodeSexCodes,
  formatDateOfBirth,
  unpackDiseaseCodes,
  unpackPhenotypicFeatures
)

def read_json(file):
  """Read JSON
  Import the contents of a json file
  """
  with open(file, 'r') as stream:
    data = json.load(stream)
  stream.close()
  return data

# set path to latest release
currentRelease = 'novelwgs_original'
pathToCurrentRelease = '/Users/davidcruvolo/Desktop/RD3/SolveRD_NovelOmics_Phenopackets'


# load corrupt files if avilable
if path.exists('corrupt_files.csv'):
  with open('currupt_files.csv', 'r') as file:
    excludedFiles = [line.replace('\n', '') for line in file.readlines()]
    file.close()  
  del file
else:
  excludedFiles = []

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Connect RD3 and Pull Reference datasets

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

# get reference datasets
hpoCodes = dt.Frame(
  rd3.get('solverd_lookups_phenotype', attributes='id')
)['id'].to_list()[0]

diseaseCodes = dt.Frame(
  rd3.get('solverd_lookups_disease', attributes='id')
)['id'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Read and process all phenopacket data
#
# Compile a list of all available files that aren't corrupted. Read and process
# each file individually. The processing step evaults the contents of all
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
# basePath = f"{environ['CLUSTER_BASE']}/{currentRelease}/phenopackets"
# allFiles = clustertools.listFiles(path=basePath)

allFiles = [ 
  { 'filename': f.strip(), 'filepath': f"{pathToCurrentRelease}/{f.strip()}" }
  for f in listdir(pathToCurrentRelease)
]

phenopacketFiles = [
  file for file in allFiles
  if (re.search(r'(.json)$', file['filename'])) and (file['filename'] not in excludedFiles)
]

print('Found', len(phenopacketFiles), 'phenopacket files')
print('Starting file processing...')
phenopackets = []

for file in tqdm(phenopacketFiles):
  # json = clustertools.readJson(path=file['filepath'])
  jsoncontents = read_json(file['filepath'])
  phenopacket = jsoncontents['phenopacket']
  result = {
    'phenopacketsID': file['filename'],
    'clusterRelease': currentRelease,
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
        if code not in hpoCodes:
          patientHpoUnknown.append(code)

    # Unobserved Phenotypes isolate unknown cases
    if patientHpoCodes['hasNotPhenotype']:
      for code in patientHpoCodes['hasNotPhenotype']:
        if code not in hpoCodes:
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
      if (code not in diseaseCodes) and (code != ''):
          patientDiseasesUnknown.append(code)

    result['disease'] = ','.join(diseases['diagnostic']) if diseases['diagnostic'] else None
    result['unknownDiseaseCodes'] = ','.join(patientDiseasesUnknown) if patientDiseasesUnknown else None

    # triage onset IDs isolate invalid codes for review
    if len(diseases['onset']) > 0:
      onset_codes_unknown = []
      for code in diseases['onset']:
        if code not in hpoCodes:
          onset_codes_unknown.append(code)

      result['ageOfOnset'] = ','.join(diseases['onset']) if diseases['onset'] else None
      result['unknownOnsetCodes'] = ','.join(onset_codes_unknown) if onset_codes_unknown else None
  
  # bind patient record to main object
  phenopackets.append(result)

# //////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data
# rd3.delete('rd3_portal_cluster_phenopacket')
# rd3_prod.delete('rd3_portal_cluster_phenopacket')

phenopacketsDT = dt.Frame(phenopackets)

# change data type void to string32
print('Checking data for void columns....')
for column in phenopacketsDT.names:
  if phenopacketsDT[column].types[0] == dt.Type.void:
    print('Changing class for column:', column)
    phenopacketsDT[column] = phenopacketsDT[column][:, dt.as_type(dt.f[column], dt.str32)]


# phenopacketsDT.to_csv('data/phenopackets_batch_1.csv')
phenopacketsDT.to_csv('data/phenopackets_batch_2.csv')


# import all batches into RD3
jsonData = dt.rbind(
  fread('data/phenopackets_batch_1.csv'),
  fread('data/phenopackets_batch_2.csv'),
)

rd3.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = jsonData
)

rd3_prod.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = jsonData
)

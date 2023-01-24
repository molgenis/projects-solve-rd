#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_phenopacket_02_validation.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-02
#' MODIFIED: 2022-09-07
#' PURPOSE: process new phenopacket data
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import statusMsg, dtFrameToRecords
from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, fread, as_type
from os import environ
from tqdm import tqdm
import re
load_dotenv()


def cleanNestedCodes(x,separator=','):
  """Clean comma separated string of HPO codes

  @param x a string containing one or more HPO code separated by a column
  @param separator A character separator if not a comma
  
  @return a clean, formatted string containing one or more HPO codes
  """
  values = x.split(separator)
  newvalues = []
  for value in values:
    newval = value.replace('\n','').replace('HP:','HP_').replace('P:', 'HP_').replace('HP0','HP_0').strip()
    if re.search(r'^(HP_)', newval):
      newvalues.append(newval)
    else:
      print('Unknown format', newval)
  return ','.join(newvalues)
  
def validateNestedCodes(x, referenceValues, separator = ','):
  """Validate comma separated codes
  
  @param x a comma separated string containing one or more values
  @param referenceValues an array of values to check
  @param separator a character separator if not a comma
  
  @return a list of unknown values
  """
  values = x.split(separator)
  unknownvalues = []
  for value in values:
    if value not in referenceValues:
      unknownvalues.append(value)
  return ','.join(unknownvalues) if unknownvalues else None

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Set up

# ~ 0a ~
# Connect to RD3: acc or prod
rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# ~ 0b ~
# Set release
# This is the Phenopacket release as indicated by the original file path
currentRelease = 'freeze3_patch1'

# ~ 0c ~
# get subject patch information to join later on
subjects = rd3.get(
  entity = 'solverd_overview',
  attributes = 'subjectID,partOfRelease',
  batch_size = 10000
)

# flatten nested attribute
for row in subjects:
  if row['partOfRelease']:
    row['partOfRelease'] = ','.join([ r['id'] for r in row['partOfRelease'] ])
  else: 
    row['partOfRelease'] = None

subjects = dt.Frame(subjects)
subjectIDs = dt.unique(subjects['subjectID']).to_list()[0]
# len(subjectIDs) == subjects.nrows
del subjects['_href']

# ~ 0d ~
# Get HPO terms
knownHpoCodes = dt.Frame(
  rd3.get(
    entity = 'rd3_phenotype',
    attributes='id',
    batch_size=10000
  )
)['id'].to_list()[0]

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Validate extracted phenopacket data and update records
# Pull data from the data from the rd3_portal. Validate subject identifier and
# merge release data.

statusMsg('Pulling latest phenopacket data....')
phenopacketDT = dt.Frame(
  rd3.get(
    entity = 'rd3_portal_cluster_phenopacket',
    batch_size = 10000
  )
)

del phenopacketDT['_href']


# ~ 1a ~
# Check to see if all subject identifiers exist in RD3
phenopacketDT['subjectExists'] = dt.Frame([
  id in subjectIDs for id in tqdm(phenopacketDT['subjectID'].to_list()[0])
])

# make sure all do! Otherwise, remove?
phenopacketDT[:, dt.count(), dt.by(f.subjectExists)]
# phenopacketDT[f.subjectExists==False, :].to_csv('data/rd3_unknown_subjects.csv')
phenopacketDT = phenopacketDT[f.subjectExists,:]

# ~ 1b ~
# Merge subject releases or the patches associated with each subject
phenopacketDT['releasesWhereSubjectExists'] = dt.Frame([
  subjects[f.subjectID==id, 'partOfRelease'].to_list()[0][0]
  for id in tqdm(phenopacketDT['subjectID'].to_list()[0])
])

# dt.unique(phenopacketDT['releasesWhereSubjectExists'])
# phenopacketDT[f.releasesWhereSubjectExists==None, :]

# import
rd3.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = phenopacketDT
)


#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Validate Data

# ~ 2a ~
# Validating date of birth
# All values in the "dateofBirth" column should be YYYY. Occasionally, the values
# are in the wrong date format or the information was entered incorrectly.

# prep DOB column for conversion to int
phenopacketDT['dateofBirth'] = dt.Frame([
  value.replace('.0', '')
  if value else None
  for value in phenopacketDT['dateofBirth'].to_list()[0]
])

phenopacketDT[:, dt.update(dateofBirth = dt.as_type(f.dateofBirth, int))]

# pull unique values and find out which values are invalid
uniqueDOB = dt.unique(phenopacketDT['dateofBirth'])

uniqueDOB['invalidFormat'] = dt.Frame([
  not bool(re.search('^([0-9]{4}$)', str(value)))
  if value else None
  for value in uniqueDOB['dateofBirth'].to_list()[0]
])


if uniqueDOB[(f.invalidFormat==True), :].nrows > 0:
  print('Warning: values in "dateofBirth" are potentially invalid. Check these values...')
  
# NOTE
# take a look at each invalid date and find in the dataset. Get the file path
# and open the file to confirm the DOB. Sometimes the date is formatted
# differently or it was entered incorrectly. Note the changes in a mapping
# file and load it here. Once you have mapped all the DOB changes, rerun the
# `uniqueDOB` steps to make sure all values have been updated. Reimport the
# data when you have confirmed all values.
#
# In the DOB mapping csv, create a column called 'newDateOfBirth' and enter the
# corrected DOBs.
uniqueDOB[
  (f.invalidFormat==True) | (f.dateofBirth > 2022) | (f.dateofBirth < 1900),
  {
    'subjectID': None,
    'dateofBirth': f.dateofBirth,
    'invalidFormat': f.invalidFormat,
    'newDateOfBirth': None
  }
].to_csv('data/date_of_birth_mappings.csv')

# find value in main dataset
phenopacketDT[f.dateofBirth=='....', :]

dobMappings = fread('data/date_of_birth_mappings.csv')

# update DOB for invalid cases
phenopacketDT['dateofBirth'] = dt.Frame([
  dobMappings[f.subjectID==tuple[0], 'newDateOfBirth'].to_list()[0][0]
  if tuple[0] in dobMappings['subjectID'].to_list()[0] else tuple[1]
  for tuple in phenopacketDT[:, (f.subjectID, f.dateofBirth)].to_tuples()
])

dt.unique(phenopacketDT['dateofBirth'])

# If everything looks good, then import into RD3
rd3.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_phenopacket',
  data = phenopacketDT
)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2b ~
# Validate disease codes
# For unknown MIM (OMIM) codes, use the following to test codes. Enter the code
# after the parameter `conceptId`:
# https://bioportal.bioontology.org/ontologies/OMIM?p=classes&conceptid=
#
# To verfiy ORDO (or Orphanet) codes, use the following search engine: 
# https://www.ebi.ac.uk/ols/ontologies/ordo. Enter codes as `Orphanet:<code>`
# into the search box
# 
# Create a new csv file and enter all new codes, and then import into RD3.

unknownDiseaseCodes = []
codes = dt.unique(phenopacketDT['unknownDiseaseCodes'])[f.unknownDiseaseCodes!=None, :].to_list()[0]
for code in codes:
  if ';' in code:
    splitCodes = code.split(';')
    for code2 in splitCodes:
      unknownDiseaseCodes.append(code2.strip())
  else:
    unknownDiseaseCodes.append(code.strip())

unknownDiseaseCodes = dt.Frame(code = unknownDiseaseCodes)

# isolate codes
unknownDiseaseCodes['codeToCheck'] = dt.Frame([
  value.split('_')[-1]
  for value in unknownDiseaseCodes['code'].to_list()[0]
])

# isolate code system
unknownDiseaseCodes['codeSystem'] = dt.Frame([
  value.split('_')[0]
  for value in unknownDiseaseCodes['code'].to_list()[0]
])

# generate URI to determine if the code exists
unknownDiseaseCodes['urlToTest'] = dt.Frame([
  f'https://bioportal.bioontology.org/ontologies/OMIM?p=classes&conceptid={row[1]}'
  if row[0] == 'MIM' else (
    f'http://www.orpha.net/ORDO/Orphanet_{row[1]}'
    if row[0] == 'ORDO' else None
  )
  for row in unknownDiseaseCodes[:, ['codeSystem', 'codeToCheck']].to_tuples()
])

# add additional columns for manual import
unknownDiseaseCodes[[
  'shouldImport',
  'id',
  'label',
  'ontology',
  'description',
  'uri',
  'parentId',
  'geneLocus',
  'geneSymbol'
]] = None

# save the data to file. Click each link to see if you are redirected to a
# valid ontology listing for the code. Double check the code listed on the page
# to determine if the record is not in RD3. It is likely that the code is new so
# it is not yet in RD3. If this is the case, copy the information into the spread-
# sheet for all new codes. If clicking a link results in an error, confirm the
# code in the URL matches the url to check. If they do, you want to manually 
# search for the code to make sure there isn't an error. If you are unable to
# find a record, then this codes does not exist or there is an error somewhere. 
# unknownDiseaseCodes.to_csv('data/unknown_disease_codes.csv')

# when finished, import the file and import into PROD and ACC
unknownDiseaseCodes = fread('data/unknown_disease_codes.csv')
rd3.importDatatableAsCsv(
  pkg_entity='solverd_lookups_disease',
  data = unknownDiseaseCodes
)
rd3_acc.importDatatableAsCsv(
  pkg_entity='solverd_lookups_disease',
  data = unknownDiseaseCodes
)


#///////////////////////////////////////////////////////////////////////////////

# ~ 2c ~
# Validate HPO codes
# Take a look at all values in the column 'unknownHpoCodes'. There should be
# a managable amount of codes to verify. However, that may not be the case.
# Issues with validiation may be a result of formatting issues, data entry
# errors, or something else. It is better, in my opinion, to clean and validate
# all codes once more, and then manually verify remaining cases. 

# take a look at the the unique caes.
dt.unique(phenopacketDT['unknownHpoCodes'])[f.unknownHpoCodes!=None,:]

# ~ 2c.i ~
# it is a better idea to clean all the columns, and then reverify each code 
# clean phenotype column
phenopacketDT['phenotype'] = dt.Frame([
  cleanNestedCodes(d) if d else d
  for d in phenopacketDT['phenotype'].to_list()[0]
])

phenopacketDT['hasNotPhenotype'] = dt.Frame([
  cleanNestedCodes(d) if d else d
  for d in phenopacketDT['hasNotPhenotype'].to_list()[0]
])

# revalidate HPO columns
phenopacketDT['unknownHpoCodes'] = dt.Frame([
  validateNestedCodes(d, knownHpoCodes) if d else d
  for d in phenopacketDT['phenotype'].to_list()[0]
])

phenopacketDT['unknownHpoCodes2'] = dt.Frame([
  validateNestedCodes(d, knownHpoCodes) if d else d
  for d in phenopacketDT['hasNotPhenotype'].to_list()[0]
])

# check values
# if unknownHpoCodes2 isn't void, collapse both columns
dt.unique(phenopacketDT['unknownHpoCodes'])[f.unknownHpoCodes!=None,:]
dt.unique(phenopacketDT['unknownHpoCodes2'])[f.unknownHpoCodes2!=None,:]

# import cleaned HPO terms
rd3.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_phenopacket', data=phenopacketDT)
rd3_acc.importDatatableAsCsv(pkg_entity='rd3_portal_cluster_phenopacket', data=phenopacketDT)


# ~ 2c.ii ~
# combine both revalidated columns. It is likely that one column may be 'void'
hpoCodesToReviewArray = dt.rbind(
  dt.unique(phenopacketDT['unknownHpoCodes'])[:, {'code':f.unknownHpoCodes}],
  # dt.unique(phenopacketDT['unknownHpoCodes2'])[:, {'code':f.unknownHpoCodes2}], # may be void
)[(f.code != None) & (f.code != ''), :].to_list()[0]

# split strings by comma and find unique codes that are unknown
hpoCodesToReview = dt.Frame()
for value in hpoCodesToReviewArray:
  for value in value.split(','):
    hpoCodesToReview = dt.rbind(hpoCodesToReview, dt.Frame(code = [value]))
hpoCodesToReview = dt.unique(hpoCodesToReview['code'])


# flag cases that do not start with 'HP_'
hpoCodesToReview['status'] = dt.Frame([
  'invalid.value' if not re.search(r'^(HP_)', code) else None
  for code in hpoCodesToReview['code'].to_list()[0]
])


# check once again to see if codes exist in `rd3_phenotype`. It is likely that the previous steps
# have fixed a lot of formatting issues that led to matching errors
hpoCodesToReview['status'] = dt.Frame([
  'code.exists' if row[0] in knownHpoCodes else row[1]
  for row in hpoCodesToReview[:, (f.code, f.status)].to_tuples()
])

# check values
dt.unique(hpoCodesToReview['status'])
hpoCodesToReview[:, dt.count(), dt.by(f.status)]

# write to file for manual review
# You can test codes by typing the following URL: http://purl.obolibrary.org/obo/<HPO_ID>

hpoCodesToReview['urlToTest'] = dt.Frame([
  'http://purl.obolibrary.org/obo/' + value
  for value in hpoCodesToReview['code'].to_list()[0]
])

hpoCodesToReview.to_csv('data/freeze3_hpo_codes2.csv')

# review all the codes by clicking the links. If you find any matches, make sure
# the information is copied (label, description, synonyms, parents, etc.) and 
# import into ACC and PROD. For any remaining codes, import them as blank
# rows.

rd3.importDatatableAsCsv(
  pkg_entity = 'rd3_phenotype',
  data = hpoCodesToReview[:, {'id': f.code, 'label': f.code}]
)
rd3_acc.importDatatableAsCsv(
  pkg_entity = 'rd3_phenotype',
  data = hpoCodesToReview[:, {'id': f.code, 'label': f.code}]
)

# once everything is fixed, update the phenopacket cluster table
if phenopacketDT['unknownHpoCodes'].type == dt.Type.void:
  phenopacketDT['unknownHpoCodes'] = dt.Frame([None], type='str32')
  
  # run only if you are confident that you know what you are doing!
  # rd3.importDatatableAsCsv(pkg_entity = 'rd3_portal_cluster_phenopacket', data = phenopacketDT)

#///////////////////////////////////////////////////////////////////////////////



# ~ 1c ~  
# Merge Patch information
# subjects['shouldMerge'] = dt.Frame([
#   d in phenopacketDT['subjectID'].to_list()[0]
#   for d in subjects['subjectID'].to_list()[0]
# ])

# subjectPatchInfo = subjects[f.shouldMerge==True,:]
# if subjectPatchInfo.nrows != phenopacketDT.nrows:
#   raise ValueError('Warning dimensions of join dataset does not match target dataset')
  
# subjectPatchInfo.key = 'subjectID'
# phenopacketDT.key = 'subjectID'
# phenopacketDT = phenopacketDT[:, :, dt.join(subjectPatchInfo['patch'])]


# # ~ 1c ~
# # Find releases
# statusMsg('Finding unique RD3 releases and preparing import object....')

# uniqueReleases = dt.unique(phenopacketDT['releasesWhereSubjectExists']).to_list()[0]

# existingReleases = []
# for release in uniqueReleases:
#   values = release.split(',')
#   if len(values) > 1:
#     for value in values:
#       if (value not in existingReleases) and (value != 'novelomics_original'):
#         existingReleases.append(value)
#   else:
#     if value != 'novelomics_original':
#       existingReleases.append(value)


# datasetsToImport = {}
# for release in existingReleases:
#   datasetsToImport[release] = {
#     'subject': {
#       'sex1': None,
#       'phenotype': None,
#       'hasNotPhenotype': None,
#       'disease': None,
#       'phenopacketsID': None,
#       'patch': None
#     },
#     'subjectinfo': {
#       'dateofBirth': None,
#       'ageOfOnset': None,
#       'patch': None
#     }
#   }

# # for each detected release prepare datasets
# for dataset in datasetsToImport:
#   statusMsg('Preparing data for',dataset,'....')
#   phenopacketDT['tmpfilter'] = dt.Frame([
#     bool(re.search(dataset, d))
#     for d in phenopacketDT['releasesWhereSubjectExists'].to_list()[0]
#   ])

#   tmpDT = phenopacketDT[f.tmpfilter,:]
#   tmpDT['subjectID'] = dt.Frame([f'{d}_original' for d in tmpDT['subjectID'].to_list()[0]])
#   tmpDT['patch'] = dt.Frame([
#     f"{d},{currentRelease}" if currentRelease not in d else d
#     for d in tmpDT['patch'].to_list()[0]
#   ])
  
#   datasetsToImport[dataset]['subject']['patch'] = tmpDT[:, (f.subjectID, f.patch)] 
#   datasetsToImport[dataset]['subject']['sex1'] = tmpDT[:, (f.subjectID, f.sex1)]
#   datasetsToImport[dataset]['subject']['phenotype'] = tmpDT[:, (f.subjectID, f.phenotype)]
#   datasetsToImport[dataset]['subject']['hasNotPhenotype'] = tmpDT[:, (f.subjectID, f.hasNotPhenotype)]
#   if 'disease' in tmpDT.names:
#     datasetsToImport[dataset]['subject']['disease'] = tmpDT[:, (f.subjectID, f.disease)]
  
#   datasetsToImport[dataset]['subjectinfo']['dateofBirth'] = tmpDT[:, (f.subjectID, f.dateofBirth)]
#   datasetsToImport[dataset]['subjectinfo']['patch'] = tmpDT[:, (f.subjectID, f.patch)]
#   if 'ageOfOnset' in tmpDT.names:
#     datasetsToImport[dataset]['subjectinfo']['ageOfOnset'] = tmpDT[:, (f.subjectID, f.ageOfOnset)]
  

# # import data
# for dataset in datasetsToImport:
#   statusMsg('Importing dataset',dataset,'....')
#   for table in datasetsToImport[dataset]:
#     statusMsg('Updating table',table,'....')
#     columns = datasetsToImport[dataset][table]
#     for column in columns:
#       pkg_entity = f"rd3_{dataset.replace('_original','')}_{table}"
#       statusMsg('Updating column',column,'in table',pkg_entity)
#       columnData = dtFrameToRecords(data=columns[column])
#       rd3.updateColumn(
#         entity=pkg_entity,
#         attr = column,
#         data = columnData
#       )
      
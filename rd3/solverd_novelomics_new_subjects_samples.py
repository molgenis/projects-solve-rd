#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_novelomics_processing.py
# AUTHOR: David Ruvolo
# CREATED: 2022-11-15
# MODIFIED: 2023-08-10
# PURPOSE: Import new novelomics data
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from datatable import dt, f, first, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
import functools
import operator
import re

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import (
  dtFrameToRecords,
  timestamp,
  toKeyPairs,
  recodeValue,
  flattenDataset
)

def getWrappedValues(value):
  """Get Wrapped Values
  In a string, extract the value between two parentheses.
  @param value a string that may or may not contain parentheses
  @return string or none
  """
  search = re.search(r'([\(].*?[\)])', value)
  if search:
    match=search.group()
    return re.sub(r'[\(\)]', '', match)
  else:
    return None
    
# init connection to RD3
load_dotenv()
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Connect to DB and get data

# get existing RD3 data
rawsubjects = rd3_prod.get(
  entity='solverd_subjects',
  attributes='subjectID,partOfRelease',
  batch_size=10000
)
subjects = flattenDataset(rawsubjects, columnPatterns="id")
subjectsDT = dt.Frame(subjects)
existingSubjectIDs = subjectsDT['subjectID'].to_list()[0]

# get existing samples
rawsamples = rd3_prod.get(
  entity='solverd_samples',
  attributes='sampleID,partOfRelease',
  batch_size=10000
)
samples = flattenDataset(rawsamples, columnPatterns="subjectID|id|value")
samplesDT = dt.Frame(samples)
existingSampleIDs = samplesDT['sampleID'].to_list()[0]

# get new shipment metadata
shipmentDT = dt.Frame(
  rd3_prod.get(
    'rd3_portal_novelomics_shipment',
    q="processed==false"
  )
)

del shipmentDT['_href']

# ~ OPTIONAL ~
# alternatively pull everything to figure out if anything is missing
# shipmentDT = dt.Frame(rd3_prod.get('rd3_portal_novelomics_shipment'))
# del shipmentDT['_href']

# shipmentDT['participant_subject'] = dt.Frame([
#   value.strip().upper() for value in shipmentDT['participant_subject'].to_list()[0]
# ])

# shipmentDT['isNewSubject'] = dt.Frame([
#   id not in existingSubjectIDs
#   for id in shipmentDT['participant_subject'].to_list()[0]
# ])

# shipmentDT['isNewSample'] = dt.Frame([
#   id not in existingSampleIDs
#   for id in shipmentDT['sample_id'].to_list()[0]
# ])

# shipmentDT[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample)]
# shipmentDT=shipmentDT[(f.isNewSubject) | (f.isNewSample), :]


#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process New Shipment metadata
# In this step, process the new shipment manifest data as a whole. Make sure 
# values are recoded into RD3 terminology (where applicable) and other columns
# are in the correct format. In the final step, determine which of the incoming
# samples already exist in RD3 as this will determine the import method.
# First, run through all lookup tables and check for new values.

# ~ 1a ~
# Detect new releases
# Use the column `type_of_analysis` to determine if the incoming data is part
# of a new or existing release. As of now, all shipment mantifests will
# contain new "releases" or updates to an existing one. It isn't necessary to 
# indicate patches for novel omics releases when importing new sample data.
releaseinfo = dt.Frame(
  rd3_prod.get(
    entity='solverd_info_datareleases',
    attributes = 'id'
  )
)['id']

releases = dt.unique(shipmentDT['type_of_analysis'])[:, {
  'id': f.type_of_analysis,
  'type': 'freeze',
  'name': f.type_of_analysis,
  'date': timestamp()
}]

# only do this once
# releases[f.id=='RNA-seq','id'] = 'SR-RNAseq'

# format IDs
releases['id'] = dt.Frame([
  f"novel{value.strip().lower().replace('-','')}_original"
  for value in releases['id'].to_list()[0]
])

releases['isNewRelease'] = dt.Frame([
  id not in releaseinfo['id'].to_list()[0]
  for id in releases['id'].to_list()[0]
])

releases['typeOfAnalysis'] = dt.Frame([
  value.lower().replace('-','')
  for value in releases['name'].to_list()[0]
])

releases['name'] = dt.Frame([
  f"Novel Omics {name}"
  for name in releases['name'].to_list()[0]
])

releaseIDs = toKeyPairs(
  data = dtFrameToRecords(data = releases),
  keyAttr = 'typeOfAnalysis',
  valueAttr = 'id'
)
 
# If there are new releases, import them 
if releases[f.isNewRelease, :].nrows > 0:
  print('There are new releases to import!!!!')
  # newPatches = releases[f.isNewRelease,f[:].remove(f.isNewRelease)]
  # newPatches['createdBy'] = 'rd3-bot'
  # rd3_prod.importDatatableAsCsv('solverd_info_datareleases', newPatches)


#///////////////////////////////////////

# ~ 1b ~ 
# Check ERNs and recode into RD3 terminology
# Make sure all unique incoming ERN values are mapped to an official ERN code
# in RD3.
erns = dt.Frame(
  rd3_prod.get(
    entity='solverd_info_erns',
    attributes='id'
  )
)['id']

ernMappings = toKeyPairs(
  data = dtFrameToRecords(data = erns),
  keyAttr='id',
  valueAttr='id'
)

# check incoming data, update mappings (if applicable), and rerun
incomingErnValues = dt.unique(shipmentDT['ERN']).to_list()[0]
for value in incomingErnValues:
  if value.lower() not in ernMappings:
    print(f'Value "{value}" not in ERN mapping dataset')

# update mappings and rerun previous incomingErnValues check until
# all mappings are resolved
ernMappings.update({
  'epicare': 'ern_epicare',
  'ern-epicare': 'ern_epicare',
  'ern-genturis': 'ern_genturis',
  'ern-ithaca': 'ern_ithaca',
  'ern-nmd': 'ern_euro_nmd',
  'genturis': 'ern_genturis',
  'ern-rita': 'ern_rita',
  'ern-rnd': 'ern_rnd',
  'ithaca': 'ern_ithaca',
  'nmd': 'ern_euro_nmd',
  'rita': 'ern_rita',
  'rnd': 'ern_rnd',
  'udn-spain': 'udn_spain'
})

#///////////////////////////////////////

# ~ 1c ~
# Check Organisation mappings
# Pull organisations to identify new values. If there are any new organisations,
# import them into RD3 and rerun step 1c.i. All values should be in RD3 before
# moving to the next step.

# ~ 1c.i ~
organisations = dt.Frame(
  rd3_prod.get(
    entity='solverd_info_organisations',
    attributes='value'
  )
)['value']

organisationMappings = toKeyPairs(
  data = dtFrameToRecords(data = organisations),
  keyAttr = 'value',
  valueAttr = 'value'
)

# check incoming data, update mappings (if applicable), and rerun
incomingOrgValues = dt.unique(shipmentDT['organisation']).to_list()[0]
for value in incomingOrgValues:
  if value not in organisationMappings:
    print(f"Value '{value}' not in Organistion mappings")

# ~ 1c.ii ~
# find organisations to import
# newOrganisations = dt.Frame({'organisation': incomingOrgValues})
# newOrganisations['isNew'] = dt.Frame([
#   value not in organisationMappings.keys()
#   for value in newOrganisations['organisation'].to_list()[0]
# ])

# newOrganisations = newOrganisations[f.isNew,'organisation']
# newOrganisations[['value', 'description']] = newOrganisations['organisation']
# del newOrganisations['organisation']

# import new organisations and rerun organisation mapping step
# rd3_acc.importDatatableAsCsv('solverd_info_organisations', newOrganisations)
# rd3_prod.importDatatableAsCsv('solverd_info_organisations', newOrganisations)

#///////////////////////////////////////

# ~ 1d ~
# Create tissue type mappings
tissueTypes = dt.Frame(
  rd3_prod.get(
    entity='solverd_lookups_tissueType',
    attributes='id'
  )
)['id']

tissueTypes['mappingID'] = dt.Frame([
  value.lower()
  for value in tissueTypes['id'].to_list()[0]
])

tissueTypeMappings = toKeyPairs(
  data = dtFrameToRecords(data = tissueTypes),
  keyAttr='mappingID',
  valueAttr='id'
)

# check incoming data, update mappings (if applicable), and rerun
incomingTissueTypes = dt.unique(shipmentDT[f.tissue_type != None, 'tissue_type']).to_list()[0]
incomingTissueTypes.sort(key=str.lower)
for value in incomingTissueTypes:
  if value.lower() not in tissueTypeMappings:
    print(f"Value '{value}' not in tissue type mappings")
    
tissueTypeMappings.update({
  'blood': 'Whole Blood',
  'cell pellet': 'Cells',
  'fibroblasts': 'Cells - Cultured fibroblasts',
  'fetus': 'Foetus',
  'ffpe': 'Tumor',
  'heart': 'Heart',
  'muscle': 'Muscle - Skeletal',
  'pbmc': 'Peripheral Blood Mononuclear Cells',
  'whole blood': 'Whole Blood'
})

#///////////////////////////////////////

# ~ 1e ~
# Create anatomical location mappings

if 'anatomical_location' in shipmentDT.names:
  print('Checking anatomical location mappings....')
  
  # As of 06 Dec 2022, the value 'blood' can be ignored as it cannot be mapped
  # to a more specific term
  anatomicalLocationMappings = {
    'chest skin': '74160004', # Skin of Chest
    'skin scalp': '43067004', # Skin of Scalp
    'right retro auricular skin': '244080005', # Entire skin of postauricular region
    'skin': '314818000', # Skin Tissue
  }

  # check incoming data, update mappings (if applicable), and rerun
  incomingAnatomicalValues = dt.unique(
    shipmentDT[f.anatomical_location != None, 'anatomical_location']
  ).to_list()[0]

  for value in incomingAnatomicalValues:
    if value.lower() not in anatomicalLocationMappings:
      print(f"Value '{value}' not in anatomical location mappings")

  # if there are mappings, update here. 
  # anatomicalLocationMappings.update({ ... })

#///////////////////////////////////////

# ~ 1f ~
# Create material type mappings
materialTypes = dt.Frame(
  rd3_prod.get(
    entity='solverd_lookups_materialType',
    attributes='id'
  )
)['id']

materialTypes['mappingID'] = dt.Frame([
  value.lower()
  for value in materialTypes['id'].to_list()[0]
])

materialTypeMappings = toKeyPairs(
  data = dtFrameToRecords(data = materialTypes),
  keyAttr = 'mappingID',
  valueAttr = 'id'
)

# check incoming data, update mappings (if applicable), and rerun
incomingMaterialTypes = dt.unique(shipmentDT[f.sample_type!=None,'sample_type']).to_list()[0]
for value in incomingMaterialTypes:
  if value.lower() not in materialTypeMappings:
    print(f"Value '{value}' does not exist in material type mappings")

materialTypeMappings.update({
  'ffpe': 'TISSUE (FFPE)',
  'total rna': 'RNA'
})

#///////////////////////////////////////

# ~ 1d ~
# Create pathological state mappings
pathologicalState = dt.Frame(
  rd3_prod.get(
    entity='solverd_lookups_pathologicalstate'
  )
)['value']

pathologicalState['mappingID'] = dt.Frame([
  value.lower()
  for value in pathologicalState['value'].to_list()[0]
])

pathologicalStateMappings = toKeyPairs(
  data = dtFrameToRecords(pathologicalState),
  keyAttr='mappingID',
  valueAttr = 'value'
)

# check incoming data, update mappings (if applicable), and rerun
if 'pathological_state' in shipmentDT.names:
  incomingPathologicalStateValues = dt.unique(
    shipmentDT[f.pathological_state!=None, 'pathological_state']
  ).to_list()[0]

  for value in incomingPathologicalStateValues:
    if value.lower() not in pathologicalStateMappings:
      print(f"Value '{value}' does not exist in pathological state mappings")

  # if there are any values, enter them below ->
  # pathologicalStateMappings.update({ ... })

#///////////////////////////////////////////////////////////////////////////////

# ~ 1e ~
# Format and recode columns and create new ones

# make sure all P numbers are uppercase (we've had some lowercase values in the past)
shipmentDT['participant_subject'] = dt.Frame([
  value.strip().upper()
  for value in shipmentDT['participant_subject'].to_list()[0]
])

# transform incoming analysis so that it can be mapped to a release
shipmentDT['type_of_analysis'] = dt.Frame([
  value.strip().lower().replace('-','') if value else value
  for value in shipmentDT['type_of_analysis'].to_list()[0]
])

shipmentDT['partOfRelease'] = dt.Frame([
  recodeValue(mappings = releaseIDs, value = value, label = 'Release')
  for value in shipmentDT['type_of_analysis'].to_list()[0]
])

shipmentDT['ERN'] = dt.Frame([
  recodeValue(mappings=ernMappings, value=value.strip().lower(), label='ERN')
  for value in shipmentDT['ERN'].to_list()[0]
])

# clean organisation
shipmentDT['organisation'] = dt.Frame([
  value.strip().lower() if value else value
  for value in shipmentDT['organisation'].to_list()[0]
])

shipmentDT['organisation'] = dt.Frame([
  re.sub(r'\s+', '-', value) if value else value
  for value in shipmentDT['organisation'].to_list()[0]
])

shipmentDT['organisation'] = dt.Frame([
  recodeValue(
    mappings=organisationMappings,
    value=value.strip().lower(),
    label='Organistion'
  )
  for value in shipmentDT['organisation'].to_list()[0]
])

# recode anatomical location (if available)
if 'anatomical_location' in shipmentDT.names:
  shipmentDT['anatomical_location'] = dt.Frame([
    recodeValue(
      mappings = anatomicalLocationMappings,
      value = value.lower(),
      label = 'anatomical locations'
    ) if value else value
    for value in shipmentDT['anatomical_location'].to_list()[0]
  ])

# recode sample types (i.e., materialType)
shipmentDT['sample_type'] = dt.Frame([
  'TISSUE (FFPE)'
  if row[0] and (row[0].lower() == 'ffpe')
  else (
    recodeValue(
      mappings = materialTypeMappings,
      value = row[1].lower(),
      label = 'Sample Type'
    ) if row[1] is not None else row[1]
  )
  for row in shipmentDT[:, (f.tissue_type, f.sample_type)].to_tuples()
])

# update sample types for DEEP-WES samples
if 'extracted_protocol' in shipmentDT.names:
  shipmentDT['sample_type'] = dt.Frame([
    'TISSUE (FFPE)'
    if row[1].lower().strip() == 'tumor' and row[2].lower().strip() == 'tissue (ffpe)'
    else row[0]
    for row in shipmentDT[:, (f.sample_type, f.tissue_type, f.extracted_protocol)].to_tuples()
  ])

# recode tissue types
shipmentDT['tissue_type'] = dt.Frame([
  recodeValue(mappings=tissueTypeMappings, value=value.strip().lower(), label='Tissue')
  for value in shipmentDT['tissue_type'].to_list()[0]
])

# extract values inside parenthesis
if 'extracted_protocol' in shipmentDT.names:
  shipmentDT['extracted_protocol'] = dt.Frame([
    # getWrappedValues(row[0]) if '(' in row[0] else row[1]
    # for row in shipmentDT[:, (f.tissue_type, f.extracted_protocol)].to_tuples()
    
    getWrappedValues(value) if value and '(' in value else value
    for value in shipmentDT[:, (f.extracted_protocol)].to_list()[0]
  ])

# add alternative identifier if not present
if 'alternative_sample_identifier' not in shipmentDT.names:
  shipmentDT['alternative_sample_identifier']=None

if 'pathological_state' in shipmentDT.names:
  shipmentDT['pathological_state'] = dt.Frame([
    recodeValue(
      mappings = pathologicalStateMappings,
      value = value.lower().strip(),
      label = 'Pathological State'
    )
    if value else None
    for value in shipmentDT['pathological_state'].to_list()[0]
  ])
else:
  shipmentDT['pathological_state'] = None

if 'tumor_cell_fraction' in shipmentDT.names:
  shipmentDT['tumor_cell_fraction'] = dt.Frame([
    None if value == 'UK' else value
    for value in shipmentDT['tumor_cell_fraction'].to_list()[0]
  ])
else:
  shipmentDT['tumor_cell_fraction'] = None

#///////////////////////////////////////

# ~ 1f ~
# Detect if there are any new subjects and samples
# New novel omics subjects and samples are submitted via the shipment manifest
# file. In this file there are three categories of records:
#
#  1) New subjects and new samples
#  2) Existing subjects with new samples
#  3) Existing subjects with existing samples
#
# It is important to identify these groups as this informs us how to process
# the data. It is highly unlikely that there will ever be a new subject with
# and old record, but it's good to check. In terms of importing the data,
# records will be imported in the following way:
#
# 1) New subjects and samples:
#     - import subject metadata into: subjects and subjectinfo
#     - import samples as is
# 2) Existing subjects with new samples
#     - update release information in subjects, subjectinfo
#     - the rest of the subject metadata can be ignored
#     - import samples as is
# 3) Existing subjects with existing samples
#     - compare changes in key attributes (sample type, material, protocol, etc)
#       to identifiy changes and to determine what to change.
#     - update release information in subjects, subjectinfo, samples
#     - the rest of the subject metadata can be ignored


# triage incoming subjects: which subjects exist in RD3?
reduceToExistingSubjects = functools.reduce(
  operator.or_,
  (f.participant_subject == value for value in existingSubjectIDs)
)

subjectsToUpdate = shipmentDT[reduceToExistingSubjects, :]
shipmentDT['isNewSubject'] = dt.Frame([
  value not in subjectsToUpdate['participant_subject'].to_list()[0]
  for value in shipmentDT['participant_subject'].to_list()[0]
])

# triage incoming samples: which samples exist in RD3?
reduceToExistingSamples = functools.reduce(
  operator.or_,
  (f.sample_id == value for value in existingSampleIDs)
)

samplesToUpdate = shipmentDT[reduceToExistingSamples, :]
shipmentDT['isNewSample'] = dt.Frame([
  id not in samplesToUpdate['sample_id'].to_list()[0]
  for id in shipmentDT['sample_id'].to_list()[0]
])

shipmentDT[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample)]
# shipmentDT[(f.isNewSubject==False) & (f.isNewSample ==False),:]

# ~ 1g ~
# Rename columns
shipmentDT.names = {
  'participant_subject': 'subjectID',
  'sample_id': 'sampleID',
  'alternative_sample_identifier': 'alternativeIdentifier',
  'tissue_type': 'tissueType',
  'sample_type': 'materialType',
  'pathological_state': 'pathologicalState',
  'tumor_cell_fraction': 'percentageTumorCells'
}

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Validate existing subjects and samples for conflicts
# For all incoming samples that already exist in RD3, it is important to check
# for any conflicts between the two records as these will need to be resolved
# independently. The following attributes must be checked.
#
# - subjectID: was the sample incorrectly associated with a subject?
# - tissueType: flag conflicts for review
# - materialType: flag conflicts for review
# - batchNumber: add if not already present
# - extractedProtocol: add if not already present
# - anatomicalLocation: flag conflicts for review
# - pathologicalState: flag conflicts for review
# - alternativeSampleIdentifiers: add if not already present
# - percentageTumorCells: flag conflicts for review
# - partOfRelease: add if not already present
#
# Some attributes can be automatically updated with out review (see notes above).

# If this step returns records, continue with the rest of the code. Otherwise, you
# may skip to the next section. :-)
samplesToValidate = shipmentDT[(f.isNewSubject==False) & (f.isNewSample==False),:]

if samplesToValidate.nrows == 0:
  print('Nothing to validate :-)')

# identify records that need to be validated
samplesToCompare = samplesDT[
  functools.reduce(
    operator.or_,
    (f.sampleID == value for value in samplesToValidate['sampleID'].to_list()[0])
  ),
  :
]

# define object that will store all conflicting data
incomingSamplesWithConflicts = []
potentialConflictColumns = [
  'subjectID',
  'tissueType',
  'materialType',
  'extractedProtocol',
  'anatomicalLocation',
  'pathologicalState',
  'percentageTumorCells',
]

# define object that will stored rows that can be updated without review
# for these cases, confirm that the incoming value does not exist for this
# record (in the string). If it does not, join the two strings.
existingSamplesThatCanBeUpdated = []
nonConflictColumns =[
  'batch',
  'alternativeSampleIdentifiers',
  'partOfRelease'
]

for id in tqdm(samplesToValidate['sampleID'].to_list()[0]):
  incomingSample = dtFrameToRecords(samplesToValidate[f.sampleID==id, :])[0]
  existingSample = dtFrameToRecords(samplesToCompare[f.sampleID==id, :])[0]

  # identify records that require manually verification
  for column in potentialConflictColumns:
    if (column in incomingSample) and (column in existingSample):
      if incomingSample[column] != existingSample[column]:
        print(f"Incoming sample {id} has conflicting {column} values")
        incomingSamplesWithConflicts.append({
          'incomingValue': incomingSample[column],
          'existingValue': existingSample[column],
          'message': f"values in {column} do not match"
        })
  
  # identify columns that can automatically imported
  for column in nonConflictColumns:
    if (column in incomingSample) and (column in existingSample):
      if incomingSample[column] not in existingSample[column]:
        newRow = {'sampleID': id, 'subjectID': incomingSample['subjectID'] }
        currentSampleValues = existingSample[column].split(',')
        currentSampleValues.append(incomingSample[column])
        newRow[column] =  ','.join(list(set(currentSampleValues)))
        existingSamplesThatCanBeUpdated.append(newRow)

numIssues=len(incomingSamplesWithConflicts)
numUpdates = len(existingSamplesThatCanBeUpdated)
print(f"Detcted {numIssues} conflict{'s'[:numIssues^1]} that require manual verification")
print(f"Detected {numUpdates} update{'s'[:numUpdates^1]} that can be imported without review")

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Import data
# Data will be imported using several methods.
#
# 1) IMPORT NEW SUBJECTS: All new subjects should be imported first. Metadata
#    should be also imported into the `subjectinfo` table. Import using the
#    `importDataTableAsCsv` method.
# 2) IMPORT NEW SAMPLES: All new samples can be imported as is. Select columns
#    and import using `importDataTableAsCsv`. Make sure release information
#    is updated in the subjects table.
# 3) UPDATING RECORDS: Using the object `existingSamplesThatCanBeUpdated`,
#    batch update each attribute.

shipmentDT['dateRecordCreated'] = timestamp()
shipmentDT['recordCreatedBy'] = 'rd3-bot'

# ~ 2a ~
# Import new subject metadata
newSubjects = shipmentDT[
  f.isNewSubject,
  (
    f.subjectID,
    f.organisation,
    f.ERN,
    f.partOfRelease,
    f.dateRecordCreated,
    f.recordCreatedBy
  )
][:, first(f[:]), dt.by(f.subjectID)]

# import into solverd_subjects
# rd3_acc.importDatatableAsCsv(pkg_entity='solverd_subjects', data = newSubjects)
rd3_prod.importDatatableAsCsv(pkg_entity='solverd_subjects', data = newSubjects)

# import into solverd_subjectinfo
# rd3_acc.importDatatableAsCsv(
#   pkg_entity='solverd_subjectinfo',
#   data = newSubjects[
#     :, (f.subjectID, f.partOfRelease, f.dateRecordCreated, f.recordCreatedBy)
#   ]
# )

rd3_prod.importDatatableAsCsv(
  pkg_entity='solverd_subjectinfo',
  data = newSubjects[
    :, (f.subjectID, f.partOfRelease, f.dateRecordCreated, f.recordCreatedBy)
  ]
)

# ~ 2b ~
# Import new sample metadata
newSamples = shipmentDT[
  f.isNewSample,
  (
    f.sampleID,
    f.alternativeIdentifier,
    f.subjectID,
    f.tissueType,
    f.materialType,
    f.pathologicalState,
    f.percentageTumorCells,
    f.partOfRelease,
    f.batch,
    f.organisation,
    f.ERN,
    f.dateRecordCreated,
    f.recordCreatedBy
  )
]

newSamples.names = {'subjectID': 'belongsToSubject'}

# rd3_acc.importDatatableAsCsv(pkg_entity='solverd_samples', data = newSamples)
rd3_prod.importDatatableAsCsv(pkg_entity='solverd_samples', data = newSamples)

# ~ 2b ~
# Update subject release information

existingSubjects = shipmentDT[f.isNewSubject==False,:]

# Is the current analysis associated with this participant?
for id in existingSubjects['subjectID'].to_list()[0]:
  subjectReleases = subjectsDT[f.subjectID==id,(f.partOfRelease)].to_list()[0][0]
  newRelease = recodeValue(
    mappings=releaseIDs,
    value=existingSubjects[f.subjectID==id,'type_of_analysis'].to_list()[0][0]
  )
  
  if newRelease not in subjectReleases:
    print('Updating releases....')
    subjectReleases = f"{subjectReleases},{newRelease}"
    subjectsDT[f.subjectID==id,(f.partOfRelease)] = subjectReleases
    subjectsDT[f.subjectID==id,'updatedRecord'] = True

updateExistingSubjects = dtFrameToRecords(
  data = subjectsDT[f.updatedRecord, (f.subjectID, f.partOfRelease)]
)

# update in subjects
# rd3_acc.batchUpdate('solverd_subjects','partOfRelease',updateExistingSubjects)
rd3_prod.batchUpdate('solverd_subjects','partOfRelease',updateExistingSubjects)

# update in subject info
# rd3_acc.batchUpdate('solverd_subjectinfo','partOfRelease',updateExistingSubjects)
rd3_prod.batchUpdate('solverd_subjects','partOfRelease',updateExistingSubjects)


# ~ 2c ~
# Update Columns
# Importing update metadata is split into two parts based on column names. Data
# will either go into the subjects or samples table.

existingSamplesThatCanBeUpdated = dt.Frame(existingSamplesThatCanBeUpdated)

# records to update in subjects
for column in existingSamplesThatCanBeUpdated.names:
  if column in ['partOfRelease']:
    print(f"Updating column {column}....")
    rd3_prod.batchUpdate(
      pkg_entity='solverd_subjects',
      column = column,
      data = dtFrameToRecords(
        data = existingSamplesThatCanBeUpdated[:, ['subjectID', column]]
      )
    )

# records to update in samples
for column in existingSamplesThatCanBeUpdated.names:
  if column in nonConflictColumns:
    print(f"Updating column {column}....")
    rd3_prod.batchUpdate(
      pkg_entity='solverd_samples',
      column=column,
      data=dtFrameToRecords(
        data = existingSamplesThatCanBeUpdated[:, ['sampleID', column]]
      )
    )

#///////////////////////////////////////

# ~ 2d ~
# Update processed status in the portal table
# Create a list of all molgenis IDs where 

portalRecordsToUpdate = dt.Frame(
  existingSamplesThatCanBeUpdated['sampleID'].to_list()[0] + newSamples['sampleID'].to_list()[0]
)
portalRecordsToUpdate.names = {'C0': 'sampleID'}

# (f.sampleID == value for value in samplesToValidate['sampleID'].to_list()[0])
molgenisIDs = shipmentDT[
  functools.reduce(
    operator.or_,
    (f.sampleID == value for value in portalRecordsToUpdate['sampleID'].to_list()[0])
  ),
  (f.sampleID, f.molgenis_id)
]

# join data
molgenisIDs.key = 'sampleID'
portalRecordsToUpdate.key = 'sampleID'
portalRecordsToUpdate = portalRecordsToUpdate[:, :, dt.join(molgenisIDs)]

portalRecordsToUpdate['processed'] = True
portalRecordsToUpdate[:, dt.update(processed = as_type(f.processed, str))]

# rd3_acc.batchUpdate(
#   pkg_entity='rd3_portal_novelomics_shipment',
#   column = 'processed',
#   data = dtFrameToRecords(portalRecordsToUpdate[:, (f.molgenis_id, f.processed)])
# )

rd3_prod.batchUpdate(
  pkg_entity='rd3_portal_novelomics_shipment',
  column = 'processed',
  data = dtFrameToRecords(portalRecordsToUpdate[:, (f.molgenis_id, f.processed)])
)

# update portal information
for rowID in portalRecordsToUpdate['molgenis_id'].to_list()[0]:
  shipmentDT[f.molgenis_id==rowID,'processed'] = True

# review cases that haven't been processed:
# Periodically, there may be a few cases that weren't marked as processed and weren't
# flagged for review. It is likely that there is additional information regarding this
# subject or sample that isn't a red flag. Make sure these cases are reviewed before
# exiting the script.
# Variables that will likely throw a message are:
#   alternativelIdentifier
#   batch
if shipmentDT[f.processed==False,:].nrows > 0:
  print('Warning: there are cases need to be reviewed!!!!')
  shipmentDT[f.processed==False,:]
  # shipmentDT[f.processed==False,'processed'] = True


#///////////////////////////////////////

# Alternatively, update all records. If you use this method, make sure you
# pull the data from each instance
# portalUpdate = dt.Frame(
#   rd3_acc.get(
#     entity='rd3_portal_novelomics_shipment',
#     q='processed==false')
# )
# portalUpdate['processed'] = 'true'

# rd3_acc.importDatatableAsCsv('rd3_portal_novelomics_shipment',portalUpdate)

# update prod
# portalUpdate = dt.Frame(
#   rd3_prod.get(
#     entity='rd3_portal_novelomics_shipment',
#     q='processed==false')
# )
# portalUpdate['processed'] = 'true'
# rd3_prod.importDatatableAsCsv('rd3_portal_novelomics_shipment',shipmentDT)


#///////////////////////////////////////

rd3_acc.logout()
rd3_prod.logout()

#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_release_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-23
#' MODIFIED: 2022-05-13
#' PURPOSE: Process new RD3 releases
#' STATUS: working
#' PACKAGES: datatable
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import recodeValue, dtFrameToRecords, statusMsg, toKeyPairs
from datatable import dt, f

# SET RELEASE INFORMATRION
portalTable = 'rd3_portal_release_new' # portal table ID
patchinfo = {
  'name': 'freeze3',                   # name of the RD3 Release
  'id': 'freeze3_original',            # ID labels `<name>_original`
  'type': 'freeze',                    # 'freeze' or 'patch'
  'date': '2022-05-09',                # Date of release, yyyy-mm-dd
  'description': 'Data Freeze 3'       # a nice description
}

# for local dev use only
from dotenv import load_dotenv
from os import environ
load_dotenv()

# host=environ['MOLGENIS_PROD_HOST']
host=environ['MOLGENIS_ACC_HOST']
rd3=Molgenis(url=host)
rd3.login(environ['MOLGENIS_ACC_USR'],environ['MOLGENIS_ACC_PWD'])

# migrate data from one server to the other:
# pull data then switch tokens and restart connection
# portalData = rd3.get(releaseName,batch_size=10000)
# rd3.importData(entity='rd3_portal_release_freeze3', data=portalData)

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Create Reference Datasets
# Pull reference tables to create mapping tables for recoding raw values into
# RD3 terminology. Add additional mappings as needed.


# ~ 0a ~
# Create ERN Mapping
erns=dt.Frame(rd3.get('solverd_info_erns'))['id']
del erns['_href']

# as key pair dictionary
ernMappings= toKeyPairs(
  data=erns[:,{'from':f.id, 'to': f.id}].to_pandas().to_dict('records'),
  keyAttr='from',
  valueAttr='to'
)

# define additional ERN mappings based on past/present values the variation
# must be mapped to an existing ERN identifier. The format you should use is:
# `'variation' : 'RD3 ERN identifier'`
ernMappings.update({
  'ERN-CRANIO': 'ern_cranio',
  'ERN-EURO-NMD': 'ern-euro-nmd',
  'ERN-EpiCARE': 'ern_epicare',
  'ERN-EuroBloodNet': 'ern_eurobloodnet',
  'ERN-EYE': 'ern_eye',
  'ERN-GUARD-HEART': 'ern_guard_heart',
  'ERN-PaedCan': 'ern_paedcan',
  'ERN-ReCONNET': 'ern_reconnet',
  'ERN-RITA': 'ern_rita',
  'Not_Applicable': None
})

# ~ 0b ~
# Create RD3 Organisations mappings
organisations=dt.Frame(rd3.get('solverd_info_organisation'))
del organisations['_href']


# define options here if necessary
# organizationMappings.update({ })


# ~ 0c ~
# Create solved status mappings
solvedStatusMappings = {
  'unsolved': False,
  'solved': True,
  'nA': None
}

# ~ 0d ~
# Create tissue type mappings
tissueTypeMappings=toKeyPairs(
  data=dt.Frame(
    rd3.get('solverd_lookups_tissueType')
  )[:,{'from': f.id, 'to':f.id}].to_pandas().to_dict('records'),
  keyAttr='from',
  valueAttr='to'
)

tissueTypeMappings.update({
  'Amniotic fluid': 'Amniotic Fluid', 
  'blood': 'Whole Blood',
  'Chorion villi': 'Chorionic Villi',
  'Peripheral blood': 'Peripheral Blood Mononuclear Cells',
  'Whole Blood': 'Whole Blood',
})

# ~ 0e ~
# Create Library Type Mappings
libraryTypeMappings=toKeyPairs(
  data=dt.Frame(
    rd3.get('solverd_lookups_libraryType')
  )[
    :, {'from': f.id, 'to': f.id}
  ].to_pandas().to_dict('records'),
  keyAttr='from',
  valueAttr='to'
)
libraryTypeMappings.update({
  'genomics': 'Genomic'
})

# ~ 0e ~
# Create Sequencer Type Mappings
seqTypeMappings=toKeyPairs(
    data=dt.Frame(
      rd3.get('solverd_lookups_seqType')
    )[
      :, {'from':f.id, 'to':f.id}
    ].to_pandas().to_dict('records'),
    keyAttr='from',
    valueAttr='to'
)

seqTypeMappings.update({
  'Whole Exome Sequencing': 'WXS',
  'Whole Genome Sequencing with or without PCR': 'WGS',
  'WGS': 'WGS',
})

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Map Portal Data
# Validate values for variables that are references

# pull data from portal
release = dt.Frame(rd3.get(portalTable, batch_size = 10000))
del release['_href']


# set primary release attributes so that it is easier to select columns later
# on in the script
# TODO: UPDATE THIS
release['partOfRelease'] = patchinfo['id']

release['sampleID'] = dt.Frame([
  f"VS{value}"
  for value in release['samples_id'].to_list()[0]
])


# ~ 1a ~
# Validate ERNS
# Make sure all ERNs values are correct. If there are any unknown ERN values,
# add them to the mappings (defined in step 0) and rerun the script up to this
# section. Repeat the process until all ERN name variations have been corrected.
# There shouldn't be any new ERNs only name variations.

# Find ERNs name variations that do not exist in RD3. If the following code
# throws any error, add the name variation to the object `ernMappings` defined
# in step 0b. Repeat until the no more mapping errors are thrown. If everything
# is mapped, then proceed to the next step.
dt.Frame([
  recodeValue(mappings=ernMappings, value=value, label="ERN")
  for value in dt.unique(
    dt.rbind(release['samples_ERN'], release['subject_ERN'], force=True)
  ).to_list()[0]
])

# recode ERNs variables with known variations (recode both columns in one go)
release[['subject_ERN','samples_ERN']] = dt.Frame([
  recodeValue(mappings=ernMappings, value=value, label='ERN')
  for value in release['subject_ERN'].to_list()[0]
])

# combine both ERNs
# rawErnData = dt.unique(
#     dt.rbind(
#         release[:, {'ERN': f.samples_ERN}],
#         release[:, {'ERN': f.subject_ERN}]
#     )
# )

# # check values
# rawErnData['ernExists'] = dt.Frame([
#     d in erns['identifier'].to_list()[0]
#     for d in rawErnData['ERN'].to_list()[0]
# ])

# # Flag cases: Records where value is None is not a problem
# if rawErnData[f.ernExists == False, :].nrows:
#     statusMsg(
#         'Error in ERN Validation:', 
#         rawErnData[f.ernExists == False, :].nrows,
#         'values do not exist: ',
#         ','.join(map(str, rawErnData[f.ernExists == False, ['ERN']].to_list()[0]))
#     )

# ~ 1b ~
# Validate Organisations
# Like the ERN mappings (step 1a), check the organisation names. These are bit
# tricky as the names are typically follow this format: <organization>_<submitter>.
# Check all values to determine if the name is new or if it is a variation of
# an existing name. Most of the time it will be a new name. If there are new
# organizations, each value should be formatted in the following way.
#   - all lowercase
#   - white spaces reduced to one
#   - replace white spaces with dash `-`
# 
# Pull new cases and import before importing the rest of the data. Apply the
# same treatment to the main dataset.


# pull unique values
rawOrgs = dt.unique(release[:, 'organisation_name'])


# check organisations
rawOrgs['orgExists'] = dt.Frame([
  value in organisations['identifier'].to_list()[0]
  for value in rawOrgs['organisation_name'].to_list()[0]
])


# flag cases
if rawOrgs[f.orgExists == False, :].nrows:
  statusMsg(
    'Error in Organisation Validation:',
    rawOrgs[f.orgExists==False, :].nrows,
    'values do not exist.',
    ','.join(map(str, rawOrgs[f.orgExists == False, f.organisation_name].to_list()[0]))
  )

# if all organisations have been reviewed, add the values to RD3 and recode
# the main dataset
newOrgs = rawOrgs[f.orgExists == False, {'name': f.organisation_name}]

# clean up values
newOrgs[['name','identifier']] = dt.Frame([
  value.strip().lower().replace(' ','-')
  for value in newOrgs['name'].to_list()[0]
])


# recode organizations if applicable
# release['organisation_name'] = dt.Frame([
#   recodeValue(mappings=organizationMappings, value=d, label='Organizations')
#   for d in release['organization_name'].to_list()[0]
# ])


# ~ 1c ~
# Validate Tissue Types
dt.Frame([
  recodeValue(mappings = tissueTypeMappings, value = value, label='Tissue Type')
  for value in release['samples_tissueType'].to_list()[0]
])

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Create RD3 Release Tables

# ~ 2a ~
# Create rd3_<release>_subject
# Not much is needed. Most of the data comes from the PED and PHENOPACKET files
subjects = release[
  :, {
    'subjectID': f.samples_subject,
    'organisation': f.subject_organisation,
    'ERN': f.subject_ERN,
    'solved': f.subject_solved,
    # 'date_solved': f.subject_date_solved, # optional: if available
    'matchMakerPermission': f.subject_matchMakerPermission,
    'recontact': f.subject_recontact,
    'partOfRelease': f.patch
  },
  dt.sort('id')
][:, dt.first(f[:]), dt.by(f.id)]

# reocde solved status
subjects['solved'] = dt.Frame([
  recodeValue(mappings=solvedStatusMappings, value=value, label='Solved status')
  for value in subjects['solved'].to_list()[0]
])


# ~ b ~
# Create rd3_<release>_subjectinfo
# There isn't much to add at this point as most of the data in this
# table comes from other sources or has never been collected. Add more column
# names here if required.

subjectInfo = subjects[:, (f.subjectID, f.partOfRelease)]

# ~ c ~
# Create rd3_<release>_sample
# Pull relevant columns for the samples table. This table is populated by data
# from the portal. Not much is needed from other sources.
samples = release[
  :, {
    'sampleID': f.sampleID,
    'sampleID': None,
    'alternativeIdentifier': f.samples_alternativeIdentifier,
    'belongsToSubject': f.subjectID,
    'tissueType': f.samples_tissueType,
    'organisation': f.subject_organisation,
    'ERN': f.samples_ERN,
    'partOfRelease': f.patch
  },
  dt.sort('sampleID')
]

# recode tisseType
samples['tissueType'] = dt.Frame([
  recodeValue(mappings = tissueTypeMappings, value = value, label='Tissue Type')
  for value in samples['tissueType'].to_list()[0]
])

# ~ d ~
# Create rd3_<release>_labinfo
labinfo = release[
  :, {
    'experimentID': f.labinfo_sample,
    'sample': f.sampleID,
    'capture': f.labinfo_capture,
    'libraryType': f.labinfo_libraryType,
    'library': f.labinfo_library,
    # 'sequencer': f.labinfo_sequencer, # if available
    'seqType': f.labinfo_seqType,
    'partOfRelease': patchinfo['id']
  }
]

# recode library type
labinfo['libraryType'] = dt.Frame([
  recodeValue(mappings=libraryTypeMappings, value=value, label='libraryType')
  for value in labinfo['libraryType'].to_list()[0]
])


# recode labinfo
labinfo['seqType'] = dt.Frame([
  recodeValue(mappings=seqTypeMappings, value=value, label='SeqType')
  for value in labinfo['seqType'].to_list()[0]
])

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data

rd3.importDatatableAsCsv(pkg_entity='solverd_lookups_organisations',data = newOrgs)

# prep data for import into RD3
rd3_subjects = dtFrameToRecords(data=subjects)
rd3_subjectInfo = dtFrameToRecords(data=subjectInfo)
rd3_samples = dtFrameToRecords(data=samples)
rd3_labinfo = dtFrameToRecords(data=labinfo)

# import data
rd3.importDatatableAsCsv(pkg_entity='solverd_subjects', data=rd3_subjects)
rd3.importDatatableAsCsv(pkg_entity='solverd_subjectinfo', data=rd3_subjectInfo)
rd3.importDatatableAsCsv(pkg_entity='solverd_samples', data=rd3_samples)
rd3.importDatatableAsCsv(pkg_entity='solverd_labinfo', data=rd3_labinfo)

# update portal
release['processed'] = True
rd3.importDatatableAsCsv(pkg_entity = portalTable, data = release)

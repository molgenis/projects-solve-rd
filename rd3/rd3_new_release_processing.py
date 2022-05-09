#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_release_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-23
#' MODIFIED: 2022-05-09
#' PURPOSE: Process new RD3 releases
#' STATUS: working
#' PACKAGES: datatable
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.utils import recodeValue, dtFrameToRecords, statusMsg, toKeyPairs
from datatable import dt, f
import functools
import operator

# SET RELEASE INFORMATRION
releaseName = 'rd3_portal_release_freeze3'   # portal table ID
patchinfo = {
    'name': 'freeze3',                  # name of the RD3 Release
    'id': 'freeze3_original',           # ID labels `<name>_original`
    'type': 'freeze',                   # 'freeze' or 'patch'
    'date': '2022-05-09',               # Date of release, yyyy-mm-dd
    'description': 'Data Freeze 3'      # a nice description
}


# for local dev use only
from dotenv import load_dotenv
from os import environ
load_dotenv()

# host=environ['MOLGENIS_HOST_ACC']
# token=environ['MOLGENIS_TOKEN_ACC']
host=environ['MOLGENIS_HOST_PROD']
token=environ['MOLGENIS_TOKEN_PROD']

# use if running in Molgenis
# host='http://localhost/api/'
# token='${molgenisToken}'

# connect to db
rd3=Molgenis(url=host, token=token)

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
erns=dt.Frame(rd3.get('rd3_ERN'))
del erns['_href']

# as key pair dictionary
ernMappings= toKeyPairs(
    data=erns[:,{'from':f.identifier, 'to': f.identifier}].to_pandas().to_dict('records'),
    keyAttr='from',
    valueAttr='to'
)

# define additional ERN mappings based on past/present values the variation
# must be mapped to an existing ERN identifier. The format you should use is:
# `'variation' : 'RD3 ERN identifier'`
ernMappings.update({
    'ERN-CRANIO': 'ERNCRANIO',
    'ERN-EURO-NMD': 'ERN-NMD',
    'ERN-EpiCARE': 'ERNEpiCARE',
    'ERN-EuroBloodNet': 'ERNEuroBloodNet',
    'ERN-EYE': 'ERNEYE',
    'ERN-GUARD-HEART': 'ERNGUARD-HEART',
    'ERN-PaedCan': 'ERNPaedCan',
    'ERN-ReCONNET': 'ERNReCONNET',
    'ERN-RITA': 'ERNRITA',
    'Not_Applicable': None
})

# ~ 0b ~
# Create RD3 Organisations mappings
organisations=dt.Frame(rd3.get('rd3_organisation'))
del organisations['_href']


# define options here if necessary
# organizationMappings.update({
#
# })


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
    data=dt.Frame(rd3.get('rd3_tissueType'))[
        :,{'from': f.identifier, 'to':f.identifier}
    ].to_pandas().to_dict('records'),
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
    data=dt.Frame(rd3.get('rd3_libraryType'))[
        :, {'from': f.identifier, 'to': f.identifier}
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
    data=dt.Frame(rd3.get('rd3_seqType'))[
        :,{'from':f.identifier,'to':f.identifier}
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
release = dt.Frame(rd3.get(releaseName, batch_size = 10000))
del release['_href']


# set primary release attributes so that it is easier to select columns later
# on in the script
release['patch'] = patchinfo['id']

release['subjectID'] = dt.Frame([
    d + '_original' for d in release['subject_id'].to_list()[0]
])

release['sampleID'] = dt.Frame([
    'VS' + d + '_original' for d in release['samples_id'].to_list()[0]
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
    recodeValue(mappings=ernMappings, value=d, label="ERN")
    for d in dt.unique(
        dt.rbind(
            release['samples_ERN'],
            release['subject_ERN'],
            force=True
        )
    ).to_list()[0]
])

# recode ERNs variables with known variations
release['samples_ERN'] = dt.Frame([
    recodeValue(mappings=ernMappings, value=d,label='ERN')
    for d in release['samples_ERN'].to_list()[0]
])

release['subject_ERN'] = dt.Frame([
    recodeValue(mappings=ernMappings, value=d, label='ERN')
    for d in release['subject_ERN'].to_list()[0]
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
    d in organisations['identifier'].to_list()[0]
    for d in rawOrgs['organisation_name'].to_list()[0]
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
    d.strip().lower().replace(' ','-')
    for d in newOrgs['name'].to_list()[0]
])


# recode organizations if applicable
# release['organisation_name'] = dt.Frame([
#     recodeValue(mappings=organizationMappings, value=d, label='Organizations')
#     for d in release['organization_name'].to_list()[0]
# ])


# ~ 1c ~
# Validate Tissue Types

dt.Frame([
    recodeValue(mappings = tissueTypeMappings, value = d, label='Tissue Type')
    for d in release['samples_tissueType'].to_list()[0]
])

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Create RD3 Release Tables

# ~ 2a ~
# Create rd3_<release>_subject
# Not much is needed. Most of the data comes from the PED and PHENOPACKET files
subjects = release[
    :,
    {
        'id': f.subjectID,
        'subjectID': f.samples_subject,
        'organisation': f.subject_organisation,
        'ERN': f.subject_ERN,
        'solved': f.subject_solved,
        # 'date_solved': f.subject_date_solved, # optional: if available
        'matchMakerPermission': f.subject_matchMakerPermission,
        'recontact': f.subject_recontact,
        'patch': f.patch
    },
    dt.sort('id')
][:, dt.first(f[1:]), dt.by(f.id)]

# reocde solved status
subjects['solved'] = dt.Frame([
    recodeValue(mappings=solvedStatusMappings,value=d,label='Solved status')
    for d in subjects['solved'].to_list()[0]
])


# ~ b ~
# Create rd3_<release>_subjectinfo
# There isn't much to add at this point as most of the data in this
# table comes from other sources or has never been collected. Add more column
# names here if required.

subjectInfo = subjects[:, (f.id, f.patch)]
subjectInfo['subjectID'] = subjectInfo['id']


# ~ c ~
# Create rd3_<release>_sample
# Pull relevant columns for the samples table. This table is populated by data
# from the portal. Not much is needed from other sources.
samples = release[
    :, {
        'id': f.sampleID,
        'sampleID': None,
        'alternativeIdentifier': f.samples_alternativeIdentifier,
        'subject': f.subjectID,
        'tissueType': f.samples_tissueType,
        'organisation': f.subject_organisation,
        'ERN': f.samples_ERN,
        'patch': f.patch
    },
    dt.sort('id')
]

# update sampleID
samples['sampleID'] = dt.Frame([
    d.replace('_original','') for d in samples['id'].to_list()[0]
])

# recode tisseType
samples['tissueType'] = dt.Frame([
    recodeValue(mappings = tissueTypeMappings, value = d, label='Tissue Type')
    for d in samples['tissueType'].to_list()[0]
])

# ~ d ~
# Create rd3_<release>_labinfo
labinfo = release[
    :,
    {
        'id': f.labinfo_sample + '_original',
        'experimentID': f.labinfo_sample,
        'sample': f.sampleID,
        'capture': f.labinfo_capture,
        'libraryType': f.labinfo_libraryType,
        'library': f.labinfo_library,
        # 'sequencer': f.labinfo_sequencer, # if available
        'seqType': f.labinfo_seqType,
        'patch': 'freeze2_original'
    }
]

# recode library type
labinfo['libraryType'] = dt.Frame([
    recodeValue(mappings=libraryTypeMappings, value=d, label='libraryType')
    for d in labinfo['libraryType'].to_list()[0]
])


# recode labinfo
labinfo['seqType'] = dt.Frame([
    recodeValue(mappings=seqTypeMappings, value=d, label='SeqType')
    for d in labinfo['seqType'].to_list()[0]
])


# ~ e ~
# Pull Molgenis IDs
# Create a dataset of all molgenis IDs that were used in the previous steps.
# This will allow us to update the portal table with new IDs.
# subjects[:, f.subjectID].to_list()[0]
# samples[:, f.sampleID].to_list()[0]
# labinfo[:, f.experimentID].to_list()[0]
# portalUpdates = release[
#     functools.reduce(
#         operator.or_, (
#             f.subject_id == id for id in subjects[:, f.subjectID].to_list()[0]
#         )
#     ), {
#         'id': f.id,
#         'processed': True 
#     }
# ]

# if portalUpdates.nrows != release.nrows:
#     raise SystemError('Error in release mapping: not all records were processed')
# else:
#     statusMsg('All records have been processed! :-)')


portalUpdates=release[:, {'id':f.id, 'processed': True}]

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data

# Update Patch table with new release info (DO THIS FIRST!)
rd3.add(
    entity = 'rd3_patch',
    data = {
        'id': patchinfo.get('id'),
        'type': patchinfo.get('type'),
        'date': patchinfo.get('date'),
        'description': patchinfo.get('description')
    }
)

# import new orgs; import ERNs if needed, but highly unlikely
rd3.importData(entity='rd3_organisation',data = dtFrameToRecords(newOrgs))

# prep data for import into RD3
rd3_subjects = dtFrameToRecords(data=subjects)
rd3_subjectInfo = dtFrameToRecords(data=subjectInfo)
rd3_samples = dtFrameToRecords(data=samples)
rd3_labinfo = dtFrameToRecords(data=labinfo)

# import data
rd3.importData(entity=f'rd3_{patchinfo["name"]}_subject', data=rd3_subjects)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_subjectinfo', data=rd3_subjectInfo)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_sample', data=rd3_samples)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_labinfo', data=rd3_labinfo)

# upodate portal
rd3.updateColumn(
    entity = releaseName,
    attr = 'processed',
    data = dtFrameToRecords(portalUpdates)
)

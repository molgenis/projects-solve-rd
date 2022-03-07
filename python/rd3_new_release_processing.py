#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_release_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-23
#' MODIFIED: 2022-03-07
#' PURPOSE: Process new RD3 releases
#' STATUS: working
#' PACKAGES: datatable
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from python.rd3tools import Molgenis, status_msg, recodeValue, to_records
from datatable import dt, f, first
import functools
import operator

# SET RELEASE INFORMATRION
release = 'rd3_portal_release_novelwgs'   # portal table ID
patchinfo = {
    'name': 'novelwgs',                   # name of the RD3 Release
    'id': 'novelwgs_original',            # ID labels `<name>_original`
    'date': '2022-03-07',                 # Date of release
    'description': None                   # define if needed
}


# for local dev use only
from dotenv import load_dotenv
from os import environ
load_dotenv()
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']

# use if running in Molgenis
# host = 'http://localhost/api/'
# token = '${molgenisToken}'

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Pull Data
status_msg('Pulling data and reference entities...')

# connect to db
rd3 = Molgenis(url=host, token=token)

# pull data from portal and reference entities
release = dt.Frame(rd3.get(release, batch_size = 10000))
release['patch'] = patchinfo.get(id)

organisations = dt.Frame(rd3.get('rd3_organisation'))
erns = dt.Frame(rd3.get('rd3_ERN'))


# ~ 0b ~
# Define Mappings

ernMappings = {
    'ERN-RITA': 'ERNRITA'
}

organizationMappings = {
    
}

solvedStatusMappings = {
    'unsolved': False,
    'solved': True
}

libraryTypeMappings = {
    'genomics': 'Genomic'
}

seqTypeMappings = {
    'Whole Exome Sequencing': 'WXS',
    'Whole Genome Sequencing with or without PCR': 'WGS',
}



#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Validate values for variables that are references

# ~ 1a ~
# Validate ERNS
# Make sure all ERNs values are correct. If there are any unknown ERN values,
# add them to the mappings (defined in step 0) and rerun the script up to this
# section. Repeat the process until all ERN name variations have been corrected.
# There shouldn't be any new ERNs only name variations.
status_msg('Looking for new ERNs...')

# recode ERNs variables with known variations
release['samples_ERN'] = dt.frame([
    recodeValue(mappings=ernMappings, value=d,label='ERN')
    for d in release['samples_ERN'].to_list()[0]
])

release['subject_ERN'] = dt.frame([
    recodeValue(mappings=ernMappings, value=d, label='ERN')
    for d in release['subject_ERN'].to_list()[0]
])

# combine both ERNs
rawErnData = dt.rbind(
    release[:, {'ERN': 'samples_ERN'}],
    release[:, {'ERN': 'subject_ERN'}]
)[:, first(f[:]), dt.by(f.ERN)]

# check values
rawErnData['ernExists'] = dt.Frame([
    d in erns['ERN'].to_list()[0]
    for d in rawErnData['ERN'].to_list()[0]
])

# Flag cases
if rawErnData[f.ernExists == False, :].nrows:
    raise status_msg(
        'Error in ERN Validation:', 
        rawErnData[f.ernExists == False, :].nrows,
        'values do not exist.',
        rawErnData[f.ernExists == False, ['ERN']].to_list()[0],
    )


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
status_msg('Looking for new organisations...')


# pull unique values
rawOrgs = dt.unique(release[:, 'organisation_name'])


# check organisations
rawOrgs['orgExists'] = dt.Frame([
    d in organisations['identifier'].to_list()[0]
    for d in rawOrgs['organisation_name'].to_list()[0]
])


# flag cases
if rawOrgs[f.orgExists == False, :].nrows:
    raise status_msg(
        'Error in Organisation Validation:',
        rawOrgs[f.orgExists==False, :].nrows,
        'values do not exist.',
        rawOrgs[f.orgExists==False, 'organisation_name'].to_list()[0]
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
release['organisation_name'] = dt.Frame([
    recodeValue(mappings=organizationMappings, value=d, label='Organizations')
    for d in release['organization_name'].to_list()[0]
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
        'id': f.samples_subject + '_original',
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
    recodeValue(mappings='',value=d,label='Solved status')
    for d in subjects['solved'].to_list()[0]
])


# ~ b ~
# Create rd3_<release>_subjectinfo
# There isn't much to add at this point as most of the data in this
# table comes from other sources or has never been collected.
subjectInfo = subjects[:, ['id', 'subjectID', 'patch']]


# ~ c ~
# Create rd3_<release>_sample
# Pull relevant columns for the samples table. This table is populated by data
# from the portal. Not much is needed from other sources.
samples = release[
    :,
    {
        'id': f.samples_id + '_original',
        'sampleID': 'VS' + f.samples_id,
        'alternativeIdentifier': f.samples_alternativeIdentifier,
        'subject': f.subject_id + '_original',
        'tissueType': f.samples_tissueType,
        'organisation': f.subject_organisation,
        'ERN': f.samples_ERN,
        'patch': f.patch
    },
    dt.sort('id')
]


# ~ d ~
# Create rd3_<release>_labinfo
labinfo = release[
    :,
    {
        'id': f.labinfo_sample + '_original',
        'experimentID': f.labinfo_sample,
        'sample': 'VS' + f.labinfo_sample + '_original',
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
portalUpdates = release[
    functools.reduce(
        operator.or_, (
            f.subject_id == id for id in subjects[:, f.subjectID].to_list()[0]
        )
    ), {
        'id': f.id,
        'processed': True 
    }
]

if len(portalUpdates) != len(release):
    raise SystemError('Error in release mapping: not all records were processed')

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data

# Update Patch table with new release info (DO THIS FIRST!)
rd3.add(
    entity = 'rd3_patch',
    data = {
        'id': patchinfo.get('id'),
        'patch_date': patchinfo.get('date'),
        'description': patchinfo.get('description')
    }
)

# prep data for import into RD3
rd3_subjects = to_records(data=subjects)

rd3_subjectInfo = to_records(data=subjectInfo)
rd3_samples = to_records(data=samples)
rd3_labinfo = to_records(data=labinfo)

# import data
rd3.importData(entity=f'rd3_{patchinfo["name"]}_subject', data=rd3_subjects)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_subjectinfo', data=rd3_subjectInfo)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_sample', data=rd3_samples)
rd3.importData(entity=f'rd3_{patchinfo["name"]}_labinfo', data=rd3_labinfo)

# upodate portal
rd3.updateColumn(
    entity = 'rd3_portal_release_freeze2',
    attr = 'processed',
    data = portalUpdates
)
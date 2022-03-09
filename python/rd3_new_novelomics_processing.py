#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_novelomics_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-03-08
#' MODIFIED: 2022-03-08
#' PURPOSE: process new novelomics data in the portal
#' STATUS: in.progress
#' PACKAGES: datatable, 
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from datetime import datetime
from multiprocessing.sharedctypes import Value
from datatable import dt, f
from python.rd3tools import (
    Molgenis,
    status_msg,
    recodeValue,
    to_keypairs,
    to_records
)
import pytz
import functools
import operator

# SET RELEASE INFORMATRION
patchinfo = {
    'name': 'novelwgs',                   # name of the RD3 Release
    'id': 'novelwgs_original',            # ID labels `<name>_original`
    'type': 'freeze',                     # 'freeze' or 'patch'
    'date': '2022-03-08',                 # Date of release, yyyy-mm-dd
    'description': 'Novel Omics WGS'      # a nice description
}

# SET EMX SUBPACKAGE IDs
novelOmicsReleases = {
    'deepwes': 'rd3_novedeepwes',
    'rnaseq': 'rd3_novelrnaseq',
    'srwgs': 'rd3_novelsrwgs',
}

# for local dev use only
from dotenv import load_dotenv
from os import environ
load_dotenv()
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

# use if running in Molgenis
# host = 'http://localhost/api/'
# token = '${molgenisToken}'

# connect to db
rd3 = Molgenis(url=host, token=token)

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Pull Data
# The source of the novelomics releases come from rd3_portal_novelomics. Data
# is sent from EGA and Tubingen, and sometimes supplied by CNAG. To run this
# script, pull both novelomics portal tables, reference entities, and create
# a list of existing subject and sample IDs.
#
# Pull mapping tables or define them below.

# ~ 0a ~
# Pull portal tables
status_msg('Pulling data from the portal')
shipment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        batch_size=10000
    )
)

experiment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_experiment',
        batch_size=10000
    )
)

del shipment['_href']
del experiment['_href']

# ~ 0b ~
# Compile existing subject-, sample-, and experiment identifiers. This
# information will be used to validate new metadata.

# get existing subjects
existingSubjects = dt.Frame()
for release in novelOmicsReleases:
    status_msg('Pulling subjects from', novelOmicsReleases[release])
    existingSubjects = existingSubjects.rbind(
        existingSubjects,
        dt.Frame(
            rd3.get(
                entity = f'{novelOmicsReleases[release]}_subject',
                attributes = 'id,subjectID,patch',
                batch_size=10000
            )
        )[:, {
            'id': f.id,
            'subjectID': f.subjectID,
            'patch': f.patch,
            'release': release
        }]
    )

# get existing samples
existingSamples = dt.Frame()
for release in novelOmicsReleases:
    status_msg('Pulling samples from', novelOmicsReleases[release])
    existingSamples = existingSamples.rbind(
        existingSamples,
        dt.Frame(
            rd3.get(
                entity = f'{novelOmicsReleases[release]}_sample',
                attributes = 'id,sampleID,patch',
                batch_size=10000
            )
        )[: {
            'id': f.id,
            'subjectID': f.sampleID,
            'patch': f.patch,
            'release': release
        }]
    )

# get existin experiments
existingExperiments = dt.Frame()
for release in novelOmicsReleases:
    status_msg('Pulling experiments from', novelOmicsReleases[release])
    existingExperiments = existingExperiments.rbind(
        existingExperiments,
        dt.Frame(
            rd3.get(
                entity = f'{novelOmicsReleases[release]}_labinfo',
                attributes = 'id,experimentID,patch',
                batch_size=10000
            )
        )[:, {
            'id': f.id,
            'experimentID': f.experimentID,
            'patch': f.patch,
            'release': release
        }]
    )

# ~ 0c ~
# Pull reference datasets and define mappings
# Define mappings to recode raw data into RD3 terminology. Make sure all
# mappings are lowered. When using the function `recodeValue`, make sure
# the input value is also lowered. Combine new mappings with existing reference
# datasets.

# ~ 0c.i ~
# Create ERN Mappings

erns = dt.Frame(rd3.get('rd3_ERN'))
erns['id'] = dt.Frame([d.lower() for d in erns['identifier'].to_list()[0]])

# convert to dictionary and append unique mappings
ernMappings = to_keypairs(
    data= to_records(
        data=erns
    ),
    keyAttr='id',
    valueAttr='identifier'
)

ernMappings.update({
    'genturis': 'ERN-GENTURIS',
    'ithaca': 'ERN-ITHACA',
    'nmd': 'ERN-NMD',
    'rnd': 'ERN-RND'
})

# ~ 0c.ii ~
# Create organisation mappings

organisations = dt.Frame(rd3.get('rd3_organisation'))
organisations['id'] = dt.Frame([
    d.lower() for d in organisations['identifier'].to_list()[0]
])

# convert to dictionary and append unique mappings
organisationMappings = to_keypairs(
    data = to_records(data = organisations),
    keyAttr = 'id',
    valueAttr = 'identifier'
)

organisationMappings.update({
    'malgorzata  dec-cwiek': 'malgorzata-dec-cwiek'
})

# ~ 0c.iii ~
# Create tissue types mappings

tissueTypes = dt.Frame(rd3.get('rd3_tissueType'))
tissueTypes['id'] = dt.Frame([
    d.lower() for d in tissueTypes['identifier'].to_list()[0]
])

# convert to dict
tissueTypeMappings = to_keypairs(
    data = to_records(data = tissueTypes),
    keyAttr='id',
    valueAttr='identifier'
)

tissueTypeMappings.update({
    'blood': 'Whole Blood',
    'ffpe': 'Tumor',
    'fibroblasts': 'Cells - Cultured fibroblasts',
    'pbmc': 'Peripheral Blood Mononuclear Cells',
    'whole blood': 'Whole Blood'
})

# ~ 0c.iv ~
# Create material type mappings

materialTypes = dt.Frame(rd3.get('rd3_materialType'))
materialTypes['id2'] = dt.Frame([
    d.lower() for d in materialTypes['id'].to_list()[0]
])

materialTypeMappings = to_keypairs(
    data = to_records(data = materialTypes),
    keyAttr = 'id2',
    valueAttr='id'
)

materialTypeMappings.update({
    'total rna': 'RNA',
    'ffpe': 'TISSUE (FFPE)'
})

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process Shipment Data and Experiment
#
# Before data is transformed into the shape of the RD3 Release structure
# merge datasets and recode values as one object. Afterwards, subset data by
# analysis type.
#

# ~ 1a ~
# Process Shipment data

# ~ 1a.i ~
# Format identifiers in the shipment dataset
# Like the main RD3 freezes, each record should receive a unique identifier
# that corresponds to the release. For example, if a record has an ID of
# "12345" and the type of analysis is "Deep-WES", then the new identifier
# should be "12345_deepwes_original". Apply the same treatment to sample- and
# subject identifiers
status_msg('Formatting identifiers...')

# make sure all identifiers have uppercase letters
shipment['participant_subject'] = dt.Frame([
    d.upper() for d in shipment['participant_subject'].to_list()[0]
])

# clean `type_of_analysis`
shipment['typeOfAnalysis'] = dt.Frame([
    d.strip().lower().replace('-','')
    for d in shipment['type_of_analysis'].to_list()[0]
])

# concat sample identifier and analysis type
shipment['sampleID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in shipment[:,['sample_id','typeOfAnalysis']].to_tuples()
])

# concat subject identifier and analysis type
shipment['subjectID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in shipment[:, ['participant_subject', 'typeOfAnalysis']].to_tuples()
])

# ~ 1a.ii ~
# Recode Attributes
# Using the mappings defined in previous step (step 0), recode the relevant
# attributes. Make sure all uses of the `recodeValue` method use `d.lower()`.
status_msg('Recoding columns...')

# recode values to align to rd3_ern
shipment['ERN'] = dt.Frame([
    recodeValue(
        mappings = ernMappings,
        value = d.lower(),
        label = 'ERN'
    )
    for d in shipment['ERN'].to_list()[0]
])


# recode values to align to rd3_organisation
shipment['organisation'] = dt.Frame([
    recodeValue(
        mappings = organisationMappings,
        value = d.lower(),
        label = 'Organisation'
    )
    for d in shipment['organisation'].to_list()[0]
])

# recode material type based on tissue type
shipment['sample_type'] = dt.Frame([
    recodeValue(
        mappings = tissueTypeMappings,
        value = d[0].lower(),
        label = 'Sample Type'
    ) if d[0].lower() == 'ffpe' else recodeValue(
        mappings = materialTypeMappings,
        value = d[1].lower(),
        label = 'Sample Type'
    )
    for d in shipment[:, (f.tissue_type, f.sample_type)].to_tuples()
])

# recode values to align to rd3_tissueType
shipment['tissue_type'] = dt.Frame([
    recodeValue(
        mappings = tissueTypeMappings,
        value = d.lower(),
        label = 'Tissue Type'
    )
    for d in shipment['tissue_type'].to_list()[0]
])


# ~ 1a.iii ~
# Triage Subjects and Samples
# Using a list of existing subject- and sample identifiers, identify new
# samples and subjects.

shipment['isNewSubject'] = dt.Frame([
    d not in existingSubjects['subjectID'].to_list()[0]
    for d in shipment['particpant_subject'].to_list()[0]
])

shipment['isNewSample'] = dt.Frame([
    d not in existingSamples['sampleID'].to_list()[0]
    for d in shipment['sample_id'].to_list()[0]
])

#///////////////////////////////////////

# ~ 1b ~
# Process Experiment Data
# 
# Apply all transformations to the experiment dataset before subsetting the
# data by type of analysis. All processing should be done in this step. If any
# data processing, that is discovered later on in the script, add them below
# and rerun the script.
# 

# ~ 1b.i ~
# Check Identifiers
#
# Format subject IDs and determine if there are any new subjects, samples,
# or experiments. Since subjects and samples are registered in RD3 before
# experiments, all experiments should be new. However, it is important to 
# confirm that there are no new cases.
status_msg('Formatting identifiers...')

# make sure subjectIDs are uppercase as there were lowercased identifiers in
# the shipment manifest dataset
experiment['subject_id'] = dt.Frame([
    d.upper() for d in experiment['subject_id'].to_list()[0]
])

experiment['typeOfAnalysis'] = dt.Frame([
    d.strip().lower().replace('-','')
    for d in experiment['experiment_type'].to_list()[0]
])

# concat sample identifier and analysis type
experiment['sampleID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in experiment[:,['sample_id','typeOfAnalysis']].to_tuples()
])

# concat subject identifier and analysis type
experiment['subjectID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in experiment[:, ['subject_id', 'typeOfAnalysis']].to_tuples()
])


# ~ 1b.ii ~
# Recode Attributes

status_msg('Transforming columns...')


# recode material type based on tissue type
experiment['sample_type'] = dt.Frame([
    recodeValue(
        mappings = tissueTypeMappings,
        value = d[0].lower(),
        label = 'Sample Type'
    ) if d[0].lower() == 'ffpe' else recodeValue(
        mappings = materialTypeMappings,
        value = d[1].lower(),
        label = 'Sample Type'
    )
    for d in experiment[:, (f.tissue_type, f.sample_type)].to_tuples()
])

# recode values to align to rd3_tissueType
experiment['tissue_type'] = dt.Frame([
    recodeValue(
        mappings = tissueTypeMappings,
        value = d.lower(),
        label = 'Tissue Type'
    )
    for d in experiment['tissue_type'].to_list()[0]
])

# recode library layout
experiment['library'] = dt.Frame([
    '1' if d == 'PAIRED' else d
    for d in experiment['library_layout'].to_list()[0]
])

# library_source to title case
experiment['library_source'] = dt.Frame([
    d.title() for d in experiment['library_source'].to_list()[0]
])

# create full path column (concat path and name)
experiment['name'] = dt.Frame([
    '/'.join(d) for d in experiment[:, (f.file_path,f.file_name)].to_tuples()
])


# ~ 1b.iv ~
# Identify new records
#
# Using the `existing*` objects collated in step 0, flag records that are new.
# For experiment metadata, all subjects and samples should already be
# registered in RD3. However, it is possible that some cases have snuck in.
# The imports for experiment data (i.e., labinfo and files) should contain data
# for existing subject and samples, as well as new experiments. If there are
# any unregistered cases, note them and send them to the SolveRD contact on 
# novel omics data.
status_msg('Checking identifiers...')

experiment['isNewSubject'] = dt.Frame([
    d not in existingSubjects['subjectID'].to_list()[0]
    for d in experiment
])

experiment['isNewSample'] = dt.Frame([
    d not in existingSamples['sampleID'].to_list()[0]
    for d in experiment['sample_id'].to_list()[0]
])

experiment['isNewExperiment'] = dt.Frame([
    d not in existingExperiments['experimentID'].to_list()[0]
    for d in experiment['project_experiment_dataset_id']
])


#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Create rd3_<release>_subject
#
# From the shipment datasets, pull all subject-level data. Since the identifier
# contains the type of analysis, all unique subjects by study will be selected.
# At this point, there all processing should have been completed in the
# previous step (see step 1). If additional transformations are needed, add
# them to step 1 and rerun.
#
# In the step below, pull selected columns and select distinct cases only (
# subject-study identifiers are already built into the ID). Using this object
# create the subject info table and subset by study.
#
# At this point, you don't have to worry about creating the subjectinfo table.
# That table will be built at import time.
#


# select columns of interest and unique rows
subjects = shipment[
    :, dt.first(f[:]), dt.by(f.subjectID)
][:,{
    'id': f.subjectID,
    'subjectID': f.participant_subject,
    'patch': patchinfo['id'],
    'organisation': f.organisation,
    'ERN': f.ERN,
    'typeOfAnalysis': f.typeOfAnalysis
}]


# subset the subjects by group (i.e., type of analysis)
# NOTE: objects for the rd3_<release>_subjectinfo tables will be created
#       at time of import.
subjectsByAnalysis = {'_nrows': {'_total': 0}}
for type in dt.unique(subjects['typeOfAnalysis']).to_list()[0]:
    dataByAnalysisType = subjects[f.typeOfAnalysis == type, :]
    subjectsByAnalysis[type] = dataByAnalysisType
    subjectsByAnalysis['_nrows'][type] = dataByAnalysisType.nrows
    subjectsByAnalysis['_nrows']['_total'] += dataByAnalysisType.nrows

# check rows    
if subjectsByAnalysis['_nrows']['_total'] != subjects.nrows:
    raise ValueError('Failed to correctly subset subjects by analysis type')
else:
    status_msg('Subsetted data by analysis type')
    
del type, dataByAnalysisType

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Build rd3_<release>_sample
#
# Select all of the columns required for the samples table. Like the previous
# step, select distinct rows using the column `sampleID` as the experiment
# type of analysis is buit into the identifiers. All transformations should
# have been completed in step 1. If additional transformations are required,
# add them into step 1 and rerun the script.
#
status_msg('Creating samples tables')

# select columns of interest and unique rows
samples = shipment[
    :, dt.first(f[:]), dt.by(f.sampleID)
][:, {
    'id': f.sampleID,
    'sampleID': f.sample_id,
    'alternativeIdentifier': f.alternative_sample_identifier,
    'subject': f.participant_subject,
    'tissueType': f.tissue_type,
    'materialType': f.sample_type,
    'patch': patchinfo['id'],
    'batch': f.batch,
    'typeOfAnalysis': f.type_of_analysis,
    'organisation': f.organisation,
    'ERN': f.ERN,
    'typeOfAnalysis': f.typeOfAnalysis
}]

# subject samples by  type of anylsis)
samplesByAnalysis = {'_nrows': {'_total': 0}}
for type in dt.unique(samples['typeOfAnalysis']).to_list()[0]:
    dataByAnalysisType = samples[f.typeOfAnalysis == type, :]
    samplesByAnalysis[type] = dataByAnalysisType
    samplesByAnalysis['_nrows'][type] = dataByAnalysisType.nrows
    samplesByAnalysis['_nrows']['_total'] += dataByAnalysisType.nrows
    
# check rows
if samplesByAnalysis['_nrows']['_total'] != samples.nrows:
    raise ValueError('Failed to correctly subset samples by analysis type')
else:
    status_msg('All samples correctly subsetted')
    
#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Build rd3_<release>_labinfo
#
# Select all of the columns required for the labinfo table. Like the previous
# two steps, select distinct rows using the column experimentID as the type
# of analysis is built into the identifiers. This will also allow you to subset
# the data based on analysis type later on.
#
# All transformations should have been completed in step 1b. If additional
# transformations are required, add them in step 1b.ii and rerun the script.
#
status_msg('Creating labinfo tables...')

# select columns of interest and unique rows
labinfo = experiment[
    :, dt.first(f[:]), dt.by(f.experimentID)
][:, {
    'id': f.experimentID,
    'experimentID': f.experiment_id,
    'sample': f.sample_id,
    'capture': f.library_selection,
    'libraryType': f.library_source,
    'library': f.library,
    'sequencingCenter': f.sequencing_center,
    'sequencer': f.platform_model,
    'seqType': f.library_strategy,
    'patch': release,
    'typeOfAnalysis': f.typeOfAnalysis
}]

# subset experiments by type of analysis
labinfoByAnalysis = {'_nrows': {'_total': 0}}
for type in dt.unique(labinfo['typeOfAnalysis']).to_list()[0]:
    dataByAnalysisType = labinfo[f.typeOfAnalysis == type, :]
    labinfoByAnalysis[type] = dataByAnalysisType
    labinfoByAnalysis['_nrows'][type] = dataByAnalysisType.nrows
    labinfoByAnalysis['_nrows']['_total'] += dataByAnalysisType.nrows
    
# check rows
if labinfoByAnalysis['_nrows']['_total'] != labinfo.nrows:
    raise ValueError('Failed to correctly subset experiments by analysis type')
else:
    status_msg('All experiments were correctly subsetted')
    

#//////////////////////////////////////////////////////////////////////////////

# ~ 5 ~
# Build rd3_<release>_files
#
# Select all of the columns required for the files table. Like the previous
# steps, select distinct rows using the column `file_name`. Since the data is
# structured by file_name, the number of distinct files should match the total
# number of rows.
#
# All transformations should have been completed in step1b. If additional
# transformations are required, add them in step 1b.ii and rerun the script.
status_msg('Creating files tables...')


# select columns of interest and unique rows
files = experiment[
    :, dt.first(f[:]), dt.by(f.name)
][:, {
    'EGA': f.file_ega_id,
    'name': f.name,
    'md5': f.unencrypted_md5_checksum,
    'typeFile': f.file_type,
    'samples': f.sample_id,
    'experimentID': f.experimentID,
    'patch': release,
    # set `dateCreated` (i.e., created in RD3 not file creation date)
    'dateCreated': datetime.now(
        tz = pytz.timezone(zone = 'Europe/Amsterdam')
    ).strftime('%Y-%m-%d'),
    'typeOfAnalysis': f.typeOfAnalysis
}]

# subset files by type of analysis
filesByAnalysis = {'_nrows': {'_total': 0}}
for type in dt.unique(files['typeOfAnalysis']).to_list()[0]:
    dataByAnalysisType = files[f.typeOfAnalysis == type, :]
    filesByAnalysis[type] = dataByAnalysisType
    filesByAnalysis['_nrows'][type] = dataByAnalysisType.nrows
    filesByAnalysis['_nrows']['_total'] += dataByAnalysisType.nrows
    
# check rows
if filesByAnalysis['_nrows']['_total'] != files.nrows:
    raise ValueError('Failed to correctly subset files by analysis type')
else:
    status_msg('All files were correctly subsetted')

#//////////////////////////////////////////////////////////////////////////////

# ~ 6 ~
# Import data

# ~ 6a ~ 
# Import data into rd3_<release>_subject and rd3_<release>_subjectinfo

subjectsByAnalysis.pop('_nrows')
for dataset in subjectsByAnalysis:
    status_msg('Importing subject data into', novelOmicsReleases[dataset])
    
    # select columns and convert to records
    rd3_subject = to_records(
        data = subjectsByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    rd3_subjectinfo = to_records(
        data = subjectsByAnalysis[dataset][:, (f.id, f.subjectID, f.patch)]
    )
    
    # import data
    rd3.importData(
        entity=f'{novelOmicsReleases[dataset]}_subject',
        data = rd3_subject
    )
    rd3.importData(
        entity=f'{novelOmicsReleases[dataset]}_subjectinfo',
        data = rd3_subjectinfo
    )

# ~ 6b ~
# Import rd3_<release>_sample

samplesByAnalysis.pop('_nrows')
for dataset in samplesByAnalysis:
    status_msg('Importing samples into', novelOmicsReleases[dataset])
    
    # convert to records
    rd3_sample = to_records(
        data = samplesByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # import
    rd3.importData(
        entity = f'{novelOmicsReleases[dataset]}_sample',
        data = rd3_sample
    )
    

# ~ 6c ~
# Import data into rd3_<release>_labinfo

labinfoByAnalysis.pop('_nrows')
for dataset in labinfoByAnalysis:
    status_msg('Importing experiments into', novelOmicsReleases[dataset])
    
    # as records
    rd3_labinfo = to_records(
        data = labinfoByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # import 
    rd3.importData(
        entity = f'{novelOmicsReleases[dataset]}_labinfo',
        data = rd3_labinfo
    )
    

# ~ 6d ~
# Import data into rd3_<release>_file

filesByAnalysis.pop('_nrow')
for dataset in filesByAnalysis:
    status_msg('Importing files into', novelOmicsReleases[dataset])
    
    # as records
    rd3_file = to_records(
        data = filesByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # import
    rd3.importData(
        entity = f'{novelOmicsReleases[dataset]}_file',
        data = rd3_file
    )
    

# !!!!! CONTINUTE HERE !!!!! #

# ~ 6e ~
# Update portal statuses

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
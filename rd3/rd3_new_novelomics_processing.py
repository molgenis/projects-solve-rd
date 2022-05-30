#//////////////////////////////////////////////////////////////////////////////
# FILE: rd3_new_novelomics_processing.py
# AUTHOR: David Ruvolo
# CREATED: 2022-03-08
# MODIFIED: 2022-05-30
# PURPOSE: process new novelomics data in the portal
# STATUS: stable
# PACKAGES: datatable, datetime, pytz, functools, operations, rd3tools
# COMMENTS: This script is designed to process Novel Omics releases and import
# them into RD3. These studies are considered separate from the main data
# freezes and they have their own EMX structure in the RD3-Molgenis instance.
# Therefore, all data will be processed separately. Please see the script
# 'rd3_new_release_processing.py' for importing new data freezes into RD3.
# In addition, see the script 'model/index.py' for creating new EMX structures.
#//////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.utils import (
    statusMsg, dtFrameToRecords, recodeValue, timestamp, toKeyPairs
)
from datatable import dt, f
import functools
import operator

# LOCAL DEV USE ONLY
# If you are running this script locally, make sure you have a valid token
# saved in the .env file.  If not, generate one and register it in the RD3-
# Molgenis database. Switch host and token when pushing data to PROD or ACC.
from dotenv import load_dotenv
from os import environ
load_dotenv()

# host=environ['MOLGENIS_PROD_HOST']
host=environ['MOLGENIS_ACC_HOST']
rd3=Molgenis(url=host)
rd3.login(
    username=environ['MOLGENIS_ACC_USR'],
    password=environ['MOLGENIS_ACC_PWD']
)

# SET EXISTING NOVELOMICS RELEASES
# Since there are many substudies in the Novel Omics space, these releases are
# imported into the their own EMX package. In
novelOmicsReleases = {
    'deepwes': 'rd3_noveldeepwes',
    'rnaseq': 'rd3_novelrnaseq',
    'srwgs': 'rd3_novelsrwgs',
}

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
# After the initial run, make sure the query param is uncommented.
statusMsg('Pulling data from the portal....')

shipment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        q='processed==False',
        batch_size=10000
    )
)

experiment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_experiment',
        q='processed==False',
        batch_size=10000
    )
)

del shipment['_href']
del experiment['_href']

# ~ 0b ~
# Build Patch Information
# Determine if there are any new releases based on type of analysis. If there
# are, stop this script and complete the following
#   1. Determine if this is an actual new study or if this data should be
#      added to an existing release
#   2. Create a new subpackage in RD3
#   3. Update the novelomics releases object at the top of this script
#   4. Rerun
#
statusMsg('Detecting new releases...')

patchinfo = dt.Frame(rd3.get('rd3_patch'))
del patchinfo['_href']

# find all analysis types (i.e., patches)
patches = dt.unique(shipment['type_of_analysis'])[:, {
    'id': f.type_of_analysis,
    'type': 'freeze',
    'name': f.type_of_analysis,
    'date' : timestamp(),
    'description': None
}]

# format IDs
patches['id'] = dt.Frame([
    f"novel{d.strip().lower().replace('-','')}_original"
    for d in patches['id'].to_list()[0]
])

patches['description'] = dt.Frame([
    f"Novel Omics {d} Data"
    for d in patches['name'].to_list()[0]
])

# detect new releases
patches['isNewRelease'] = dt.Frame([
    d not in patchinfo['id'].to_list()[0]
    for d in patches['id'].to_list()[0]
])


# import new patches
# newPatches = to_records(patches[f.isNewRelease, f[:].remove(f.isNewRelease)])
# rd3.importData(entity = 'rd3_patch', data=newPatches)

# add typeofanalysis
patches['typeOfAnalysis'] = dt.Frame([
    d.lower().replace('-','')
    for d in patches['name'].to_list()[0]
])

# create mapping table
patchIDs = toKeyPairs(
    data=dtFrameToRecords(data=patches),
    keyAttr='typeOfAnalysis',
    valueAttr='id'
)

#///////////////////////////////////////

# ~ 0c ~
# Compile existing subject-, sample-, and experiment identifiers. This
# information will be used to validate new metadata.
statusMsg('Compiling lists of existing identifiers....')

# get existing subject metadata
existingSubjects = dt.Frame()
for release in novelOmicsReleases:
    statusMsg('Pulling subjects from', novelOmicsReleases[release])
    tmpSubjectData = rd3.get(
        entity = f'{novelOmicsReleases[release]}_subject',
        attributes = 'id,subjectID,patch',
        batch_size=10000
    )
    for row in tmpSubjectData:
        if 'patch' in row:
            row['patch']= ','.join([patch['id'] for patch in row['patch']])
    
    tmpSubjectData=dt.Frame(tmpSubjectData)[:, {
        'id': f.id,
        'subjectID': f.subjectID,
        'patch': f.patch,
        'release': release
    }]
    
    existingSubjects = dt.rbind(existingSubjects, tmpSubjectData)

# get existing sample metadata
existingSamples = dt.Frame()
for release in novelOmicsReleases:
    statusMsg('Pulling samples from', novelOmicsReleases[release])
    tmpSampleData=rd3.get(
        entity = f'{novelOmicsReleases[release]}_sample',
        attributes = 'id,sampleID,patch',
        batch_size=10000
    )
    
    for row in tmpSampleData:
        if 'patch' in row:
            row['patch']=','.join([patch['id'] for patch in row['patch']])
    
    tmpSampleData=dt.Frame(tmpSampleData)[:, {
        'id': f.id,
        'sampleID': f.sampleID,
        'patch': f.patch,
        'release': release
    }]
    
    existingSamples = dt.rbind(existingSamples,tmpSampleData)

# get existing experiment metadata
existingExperiments = dt.Frame()
for release in novelOmicsReleases:
    statusMsg('Pulling experiments from', novelOmicsReleases[release])
    tmpExperimentData=rd3.get(
        entity = f'{novelOmicsReleases[release]}_labinfo',
        attributes = 'id,experimentID,patch',
        batch_size=10000
    )
    
    for row in tmpExperimentData:
        if 'patch' in row:
            row['patch']=','.join([patch['id'] for patch in row['patch']])
            
    tmpExperimentData=dt.Frame(tmpExperimentData)[:, {
        'id': f.id,
        'experimentID': f.experimentID,
        'patch': f.patch,
        'release': release
    }]
    
    existingExperiments = dt.rbind(existingExperiments,tmpExperimentData)


#///////////////////////////////////////

# ~ 0d ~
# Pull reference datasets and define mappings
# Define mappings to recode raw data into RD3 terminology. Make sure all
# mappings are lowered. When using the function `recodeValue`, make sure
# the input value is also lowered. Combine new mappings with existing reference
# datasets.
statusMsg('Defining mapping tables....')

# ~ 0d.i ~
# Create ERN Mappings

erns = dt.Frame(rd3.get('rd3_ERN'))
erns['id'] = dt.Frame([d.lower() for d in erns['identifier'].to_list()[0]])

# convert to dictionary and append unique mappings
ernMappings = toKeyPairs(
    data= dtFrameToRecords(data=erns),
    keyAttr='id',
    valueAttr='identifier'
)

ernMappings.update({
    'genturis': 'ERN-GENTURIS',
    'ithaca': 'ERN-ITHACA',
    'nmd': 'ERN-NMD',
    'rnd': 'ERN-RND'
})

del erns

# ~ 0d.ii ~
# Create organisation mappings

organisations = dt.Frame(rd3.get('rd3_organisation'))
organisations['id'] = dt.Frame([
    d.lower() for d in organisations['identifier'].to_list()[0]
])

# convert to dictionary and append unique mappings
organisationMappings = toKeyPairs(
    data = dtFrameToRecords(data = organisations),
    keyAttr = 'id',
    valueAttr = 'identifier'
)

organisationMappings.update({
    'malgorzata  dec-cwiek': 'malgorzata-dec-cwiek',
    'cheo-lochmuller ': 'cheo-lochmuller',
    'lafe-vilchez': 'lafe-vilchez'
})

del organisations

# ~ 0d.iii ~
# Create tissue types mappings

tissueTypes = dt.Frame(rd3.get('rd3_tissueType'))
tissueTypes['id'] = dt.Frame([
    d.lower() for d in tissueTypes['identifier'].to_list()[0]
])

# convert to dict
tissueTypeMappings = toKeyPairs(
    data = dtFrameToRecords(data = tissueTypes),
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

del tissueTypes

# ~ 0d.iv ~
# Create material type mappings

materialTypes = dt.Frame(rd3.get('rd3_materialType'))
materialTypes['id2'] = dt.Frame([
    d.lower() for d in materialTypes['id'].to_list()[0]
])

materialTypeMappings = toKeyPairs(
    data = dtFrameToRecords(data = materialTypes),
    keyAttr = 'id2',
    valueAttr='id'
)

materialTypeMappings.update({
    'total rna': 'RNA',
    'ffpe': 'TISSUE (FFPE)'
})

del materialTypes

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
statusMsg('Processing shipment metadata....')

# ~ 1a.i ~
# Format identifiers in the shipment dataset
# Like the main RD3 freezes, each record should receive a unique identifier
# that corresponds to the release. For example, if a record has an ID of
# "12345" and the type of analysis is "Deep-WES", then the new identifier
# should be "12345_deepwes_original". Apply the same treatment to sample- and
# subject identifiers
statusMsg('Formatting identifiers....')

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
statusMsg('Recoding columns...')


# add patch
shipment['patch'] = dt.Frame([
    recodeValue(
        mappings = patchIDs,
        value = d,
        label = 'Patch'
    )
    for d in shipment['typeOfAnalysis'].to_list()[0]
])

# recode values to align to rd3_ern
shipment['ERN'] = dt.Frame([
    recodeValue(
        mappings = ernMappings,
        value = d.strip().lower(),
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
    'TISSUE (FFPE)' if d[0].lower() == 'ffpe' else recodeValue(
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


# set releases to match existingSubjects.release
shipment['release'] = dt.Frame([
    d.lower().replace('-','')
    for d in shipment['type_of_analysis'].to_list()[0]
])


# ~ 1a.iii ~
# Triage Subjects and Samples
# Using a list of existing subject- and sample identifiers, identify new
# samples and subjects.

shipment['isNewSubject'] = dt.Frame([
    existingSubjects[(f.subjectID == row[0]) & (f.release == row[1]), :].nrows == 0
    for row in shipment[:, ['participant_subject','release']].to_tuples()
])

shipment['isNewSample'] = dt.Frame([
    existingSamples[(f.sampleID == row[0]) & (f.release==row[1]), :].nrows == 0
    for row in shipment[:,['sample_id','release']].to_tuples()
])

#///////////////////////////////////////

# ~ 1b ~
# Process Experiment Data
# 
# Apply all transformations to the experiment dataset before subsetting the
# data by type of analysis. All processing should be done in this step. If any
# data processing, that is discovered later on in the script, add them below
# and rerun the script.

statusMsg('Processing experiment manifest file....')

# ~ 1b.i ~
# Check Identifiers
#
# Format subject IDs and determine if there are any new subjects, samples,
# or experiments. Since subjects and samples are registered in RD3 before
# experiments, all experiments should be new. However, it is important to 
# confirm that there are no new cases.
statusMsg('Formatting identifiers...')

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

# create experiment_id (primary key)
experiment['experiment_id'] = dt.Frame([
    f"{d}_original"
    for d in experiment['project_experiment_dataset_id'].to_list()[0]
])


# ~ 1b.ii ~
# Recode Attributes

statusMsg('Transforming columns...')

# add patch
experiment['patch'] = dt.Frame([
    recodeValue(
        mappings = patchIDs,
        value = d,
        label = 'Patch'
    )
    for d in experiment['typeOfAnalysis'].to_list()[0]
])

# recode material type based on tissue type
experiment['sample_type'] = dt.Frame([
    'TISSUE (FFPE)' if d[0].lower() == 'ffpe' else recodeValue(
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
statusMsg('Checking identifiers...')

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
# We only need to select new subjects

# select columns of interest and unique rows
subjects = shipment[
    f.isNewSubject==True,:
][
    :, dt.first(f[:]), dt.by(f.subjectID)
][:,{
    'id': f.subjectID,
    'subjectID': f.participant_subject,
    'patch': f.patch,
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
    statusMsg('Subsetted data by analysis type')
    
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
statusMsg('Creating samples tables')

# select columns of interest and unique rows
samples = shipment[
    f.isNewSample==True,:
][
    :, dt.first(f[:]), dt.by(f.sampleID)
][:, {
    'id': f.sampleID,
    'sampleID': f.sample_id,
    'alternativeIdentifier': f.alternative_sample_identifier,
    'subject': f.subjectID,
    'tissueType': f.tissue_type,
    'materialType': f.sample_type,
    'patch': f.patch,
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
    statusMsg('All samples correctly subsetted')
    
del type, dataByAnalysisType

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
statusMsg('Creating labinfo tables...')

# select columns of interest and unique rows
labinfo = experiment[
    :, dt.first(f[:]), dt.by(f.experiment_id)
][:, {
    'id': f.experiment_id,
    'experimentID': f.project_experiment_dataset_id,
    'sample': f.sampleID,
    'capture': f.library_selection,
    'libraryType': f.library_source,
    'library': f.library,
    'sequencingCenter': f.sequencing_center,
    'sequencer': f.platform_model,
    'seqType': f.library_strategy,
    'patch': f.patch,
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
    statusMsg('All experiments were correctly subsetted')
    
del type, dataByAnalysisType

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
statusMsg('Creating files tables...')


# select columns of interest and unique rows
files = experiment[
    :, dt.first(f[:]), dt.by(f.name)
][:, {
    'EGA': f.file_ega_id,
    'name': f.name,
    'md5': f.unencrypted_md5_checksum,
    'typeFile': f.file_type,
    'samples': f.sampleID,
    'experimentID': f.experiment_id,
    'patch': f.patch,
    # set `dateCreated` (i.e., created in RD3 not file creation date)
    'dateCreated': timestamp(),
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
    statusMsg('All files were correctly subsetted')

#//////////////////////////////////////////////////////////////////////////////

# ~ 6 ~
# Import data

# ~ 6a ~ 
# Import data into rd3_<release>_subject and rd3_<release>_subjectinfo
subjectsByAnalysis.pop('_nrows')
for dataset in subjectsByAnalysis:
    statusMsg('Importing subject data into', novelOmicsReleases[dataset])
    
    # convert to records
    rd3_subject = dtFrameToRecords(
        data = subjectsByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # create subject info
    rd3_subjectinfo = dtFrameToRecords(
        data = subjectsByAnalysis[dataset][
            :, {
                'id': f.id,
                'subjectID': f.id,
                'patch': f.patch
            }
        ]
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
    statusMsg('Importing samples into', novelOmicsReleases[dataset])
    
    # convert to records
    rd3_sample = dtFrameToRecords(
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
    statusMsg('Importing experiments into', novelOmicsReleases[dataset])
    
    # as records
    rd3_labinfo = dtFrameToRecords(
        data = labinfoByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # import 
    rd3.importData(
        entity = f'{novelOmicsReleases[dataset]}_labinfo',
        data = rd3_labinfo
    )
    

# ~ 6d ~
# Import data into rd3_<release>_file
filesByAnalysis.pop('_nrows')
for dataset in filesByAnalysis:
    statusMsg('Importing files into', novelOmicsReleases[dataset])
    
    # as records
    rd3_file = dtFrameToRecords(
        data = filesByAnalysis[dataset][:, f[:].remove(f.typeOfAnalysis)]
    )
    
    # import
    rd3.importData(
        entity = f'{novelOmicsReleases[dataset]}_file',
        data = rd3_file
    )


# ~ 6e ~
# Update portal statuses
#
# Detect which records from the portal tables were processed and pull the
# corresponding row identifier (`molgenis_id`). Each portal table uses an auto
# row ID so that external collaborators can import the raw data with ease. In
# this last step, find the row identifiers based on the processed subjects so
# that these values can be updated.
statusMsg('Updating portal statuses....')

# ~ 6e.i ~
# update shipment table
shipmentUpdates = shipment[
    functools.reduce(
        operator.or_, (
            f.participant_subject == id for id in subjects[:,f.subjectID].to_list()[0]
        )
    ), {
        'molgenis_id': f.molgenis_id,
        'processed': True 
    }
]

# check updates
if shipmentUpdates.nrows != shipment.nrows:
    statusMsg(
        'Not all records were processed. There are',
        shipment.nrows - shipmentUpdates.nrows,
        'records remaining.'
    )
else:
    statusMsg('All records were processed! :-)')

# import
rd3_shipment_updates = dtFrameToRecords(shipmentUpdates)
rd3.updateColumn(
    entity = 'rd3_portal_novelomics_shipment',
    attr = 'processed',
    data = rd3_shipment_updates
)


# ~ 6e.ii ~
# update experiment table

experimentUpdates = experiment[
    functools.reduce(
        operator.or_, (
            f.project_experiment_dataset_id == id for id in labinfo[:, f.experimentID].to_list()[0]
        )
    ), {
        'molgenis_id': f.molgenis_id,
        'processed': True
    }
]

# check processed rows
if experimentUpdates.nrows != experiment.nrows:
    statusMsg(
        'Not all records were processed. There are still',
        experiment.nrows - experimentUpdates.nrows,
        'records remaining.'        
    )
else:
    statusMsg('All records were processed! :-)')

rd3_experiment_updates = dtFrameToRecords(data=experimentUpdates)
rd3.updateColumn(
    entity = 'rd3_portal_novelomics_experiment',
    attr = 'processed',
    data = rd3_experiment_updates
)

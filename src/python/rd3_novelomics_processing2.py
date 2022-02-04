# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2022-02-04
# PURPOSE: process novel omics into Solve RD
# STATUS: experimental
# DEPENDENCIES: **see below**
# COMMENTS: The purpose of this script is to map novel omics metadata into
# main RD3 tables. Data is uploaded by external partners and this workflow
# maps values into RD3 terminology and imports them into the correct freeze.
# At the moment, this script covers Freeze 1 & 2. If new data does not exist
# in either freeze, then records are pushed into a 'holding' table. This script
# will likely need to be updated to pull and process data from the 'holding'
# table. If more freezes are added, adjust the script accordingly.
# //////////////////////////////////////////////////////////////////////////////

from src.python.utils import molgenis, status_msg
from datatable import dt,f, first
from dotenv import load_dotenv
from datetime import datetime
from os import environ
import numpy as np

# set molgenis.client info
load_dotenv()
# host = environ['MOLGENIS_HOST_ACC']
# token = environ['MOLGENIS_TOKEN_ACC']
host = environ['MOLGENIS_HOST_PROD']
token = environ['MOLGENIS_TOKEN_PROD']
rd3 = molgenis(url=host, token=token)


# set current release information
release = 'novelomics_original' # ex: 'freezeN_original' or 'freezeN_patchX'
experimentIdSuffix = '_original'
sampleIdSuffix = '_original'


# datatable frame to list/dicts
def toRecords(data):
    """To Records
    Convert a datatable frame to a list of dictionaries
    @param data (obj) : a datatable object
    """
    return data.to_pandas().replace({np.nan:None}).to_dict('records')


# define Organisation mappings
def __validate__(value, patterns):
    try:
        return patterns[value.strip().lower()]
    except:
        pass
    return value.strip()

def recodeErns(value):
    """Recode ERNs
    Recode known ERN name variations into RD3 terminology.
    @param value (str) : value to recode
    @return string
    """
    patterns = {
        'genturis': 'ERN-GENTURIS',
        'ithaca': 'ERN-ITHACA',
        'nmd': 'ERN-NMD',
        'rnd': 'ERN-RND'
    }
    return __validate__(value, patterns)

def recodeOrganisations(value):
    """Recode Organisations
    Recode known organisation name variations into RD3 terminology.
    @param value (str) : value to recode
    @return string
    """
    patterns = {
        'malgorzata  dec-cwiek': 'malgorzata-dec-cwiek'    
    }    
    return __validate__(value, patterns)

def recodeTissueTypes(value):
    """Recode Tissue Type
    Recode tissue type name variations into RD3 terminology
    @param value (str) : value to recode
    @return string
    """
    patterns = {
        'blood': 'Whole Blood',
        'ffpe': 'Tumor',
        'fibroblasts': 'Cells - Cultured fibroblasts',
        'pbmc': 'Peripheral Blood Mononuclear Cells',
        'whole blood': 'Whole Blood'
    }
    return __validate__(value, patterns)

def recodeMaterialTypes(value):
    """Recode Material Types
    Recode known material type variations into RD3 terminology
    @param value (str) : value to recode
    @return string
    """
    patterns = {
        'total rna': 'RNA',
        'ffpe': 'TISSUE (FFPE)'
    }
    return __validate__(value, patterns)

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull Required Data
#
# For the novel omics mapping job, we need to pull all unprocessed novel omics
# data located in the portal (shipment and experiment).

status_msg('Looking for new subjects and experiments to processe...')

# ~ 1a ~
# Pull Portal Data

# pull `rd3_portal_novelomics_experiment`
newExperiments = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_experiment',
        q='processed==false',
        batch_size=10000
    )
)

if newExperiments.nrows:
    del newExperiments['_href']


# pull `rd3_portal_novelomics_shipment` (subjects and samples)
newSamples = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        q='processed==false',
        batch_size=10000
    )
)

# isolate attributes for updating the portal post-processing
portalShipmentMetadata = newSamples[
    :, [f.molgenis_id, f.participant_subject, f.sample_id, f.processed]
]

if newSamples.nrows:
    del newSamples['_href']

#//////////////////////////////////////

# ~ 1b ~
# Pull existing rd3_novelomics datasets for reference
#
# Before new subjects, samples, and experiments can be imported into RD3, it
# is important to make sure that they do not already exist in RD3, especially
# subjects and samples. If a record does exist, it can be ignored or you can
# run additional checks to make sure the records should be updated. This is
# highly unlikely though as all releases contain new samples. In the future,
# there may be updates.
#
status_msg('Fetching existing data for comparison...')

# get `rd3_novelomics_subjects`
existingSubjects = rd3.get(
    entity='rd3_novelomics_subject',
    attributes='id,subjectID,patch,ERN,organisation',
    batch_size=10000
)

for subject in existingSubjects:
    subject['ERN'] = subject.get('ERN', {}).get('identifier')
    subject['organisation'] = subject.get('organisation',{}).get('identifier')
    subject['patch'] = ','.join([d['id'] for d in subject['patch']])

existingSubjects = dt.Frame(existingSubjects)
existingSubjectIDs = existingSubjects['subjectID'].to_list()[0]
del subject
del existingSubjects['_href']

# get `rd3_novelomics_samples`
existingSamples = rd3.get(
    entity = 'rd3_novelomics_sample',
    attributes='id,sampleID,subject'    
)

for sample in existingSamples:
    sample['subjectKey'] = sample['subject'].get('id')
    sample['subjectID'] = sample['subject'].get('subjectID')
    del sample['subject']
    
existingSamples = dt.Frame(existingSamples)
existingSampleIDs = existingSamples['sampleID'].to_list()[0]
del sample
del existingSamples[:, '_href']


# get `rd3_novelomics_labinfo_wgs`
existingExperiments = rd3.get(
    entity = 'rd3_novelomics_labinfo_wgs',
    attributes='id,experimentID,sample',
    batch_size=10000
)

for experiment in existingExperiments:
    experiment['sampleKey'] = experiment.get('sample')[0].get('id')
    experiment['sampleID'] = experiment.get('sample')[0].get('sampleID')
    del experiment['sample']

existingExperiments = dt.Frame(existingExperiments)
existingExperimentIDs = existingExperiments['experimentID'].to_list()[0]
del experiment
del existingExperiments['_href']


#//////////////////////////////////////

# ~ 1c ~
# Pull other reference datasets for validation

rd3_ern = dt.Frame(rd3.get('rd3_ERN',attributes='identifier'))
rd3_organisation = dt.Frame(rd3.get('rd3_organisation',attributes='identifier'))

ernIDs = rd3_ern['identifier'].to_list()[0]
organisationsIDs = rd3_organisation['identifier'].to_list()[0]

del rd3_ern[:,'_href']
del rd3_organisation[:,'_href']

#//////////////////////////////////////

# ~ 1d ~
# Determine which manifest needs to be mapped

# set processing and import flags
should_process_experiments = False
should_process_samples = False

should_import_subjects = False
should_import_subjectinfo = False
should_import_labs_wgs = False
should_import_files = False
# should_import_labs_rna = False

should_import_organisations = False
should_import_erns = False


if newExperiments and newSamples:
    status_msg(
        'Will process {} new experiments and {} samples'
        .format(newExperiments.nrows, newSamples.nrows)
    )
    should_process_experiments = True
    should_process_samples = True
elif newExperiments and not newSamples:
    status_msg(
        'Will process {} new experiments and 0 samples'
        .format(newExperiments.nrows)
    )
    should_process_experiments = True
elif not newExperiments and newSamples:
    status_msg(
        'Will process 0 new experiments and {} samples'
        .format(newSamples.nrows)
    )
    should_process_samples = True
else:
    status_msg('Everything is up to date. :-)')

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process New Samples and Subjects
#
# The processing and importing of patient and sample metadata were separated
# from experiment metadata. It was reported that the sample IDs and experiment
# identifiers in the sample manifest were not always correct, and that
# the sample manifest is always submitted before the experiment manifest. It
# was decided that new subjects and samples could be registered in 
# `rd3_novelomics` before experiment metadata. Should any incorrect identifiers
# be identified, these could be resolved case by case.
#
# Based on the value of `should_map_samples`, the following step prepares data
# for `rd3_novelomics_subjects` and `rd3_novelomics_samples`. At the end of
# this step, `should_import_subjects` and `should_import_samples` is set to
# True which will signal that the data should be imported in step 4 of this
# script.

if should_process_samples:
    status_msg('Processing samples and subjects...')

    newSamples['isNewSubject'] = dt.Frame([
        d not in existingSubjectIDs
        for d in newSamples['participant_subject'].to_list()[0]
    ])
    
    newSamples['isNewSample'] = dt.Frame([
        d not in existingSampleIDs
        for d in newSamples['sample_id'].to_list()[0]
    ])

    newSamples['ERN'] = dt.Frame([
        recodeErns(d)
        for d in newSamples['ERN'].to_list()[0]
    ])

    newSamples['organisation'] = dt.Frame([
        recodeOrganisations(d)
        for d in newSamples['organisation'].to_list()[0]
    ])
    
    #//////////////////////////////////
    
    # ~ 2a ~
    # Prepare data for `rd3_novelomics_subject`
    # New subjects will be added to RD3.

    if newSamples[f.isNewSubject, :].nrows:
        status_msg('Preparing new subjects for registration into RD3...')
        
        newNovelOmicsSubjects = newSamples[f.isNewSubject == True, :][
            :, first(f[:]), dt.by(f.participant_subject) 
        ][:,{
            'id': f.participant_subject,
            'subjectID': f.participant_subject,
            'patch': release,
            'organisation': f.organisation,
            'ERN': f.ERN
        }]
    
        newNovelOmicsSubjects['id'] = dt.Frame([
            f'{d}_{release}' for d in newNovelOmicsSubjects['id'].to_list()[0]
        ])
        
        should_import_subjects = True
        should_import_subjectinfo = True
    
    #//////////////////////////////////
    
    # ~ 2a ~
    # Prepare data for `rd3_novelomics_sample`
    
    # only process new samples
    if newSamples[f.isNewSample, :].nrows:
        status_msg('Preparing new samples for registration in RD3...')
            
        newNovelOmicsSamples = newSamples[f.isNewSample,:][
            :, first(f[:]), dt.by(f.sample_id)  
        ][:, {
            'id': f.sample_id,
            'sampleID': f.sample_id,
            'alternativeIdentifier': f.alternative_sample_identifier,
            'subject': f.participant_subject,
            'tissueType': f.tissue_type,
            'materialType': f.sample_type,
            'patch': release,
            'batch': f.batch,
            'typeOfAnalysis': f.type_of_analysis,
            'organisation': f.organisation,
            'ERN': f.ERN
        }]
            
        # set entity row ID
        newNovelOmicsSamples['id'] = dt.Frame([
            f'{d}_{release}'
            for d in newNovelOmicsSamples['id'].to_list()[0]
        ])
            
        # set subject ID to the current release
        newNovelOmicsSamples['subject'] = dt.Frame([
            f'{d}_{release}'
            for d in newNovelOmicsSamples['subject'].to_list()[0]
        ])
        
        
        # concat multiple alternativeIdentifiers
        newNovelOmicsSamples['alternativeIdentifier'] = dt.Frame([
            ', '.join(
                newSamples[
                    f.sample_id == d,
                    f.alternative_sample_identifier
                ].to_list()[0]
            ) if newSamples[
                (f.sample_id == d) & (f.alternative_sample_identifier != None), 
                f.alternative_sample_identifier
            ].nrows else None
            for d in newNovelOmicsSamples['sampleID'].to_list()[0]
        ])
        
        # concat multiple batches
        newNovelOmicsSamples['batch'] = dt.Frame([
            ', '.join(
                newSamples[
                    f.sample_id == d,
                    f.batch
                ].to_list()[0]
            ) if newSamples[
                (f.sample_id == d) & (f.batch != None),
                f.batch
            ].nrows else None
            for d in newNovelOmicsSamples['sampleID'].to_list()[0]
        ])
        
        # recode materialType first for cases where tissueType is
        # FFPE
        newNovelOmicsSamples['materialType'] = dt.Frame([
            recodeMaterialTypes(d[0]) if d[0] == 'FFPE' else recodeMaterialTypes(d[1])
            for d in newNovelOmicsSamples[
                :, [f.tissueType, f.materialType]
            ].to_tuples()
        ])
        
        
        # recode tissue types
        newNovelOmicsSamples['tissueType'] = dt.Frame([
            recodeTissueTypes(d)
            for d in newNovelOmicsSamples['tissueType'].to_list()[0]
        ])  
        
        # set import flag
        should_import_samples=True
     
    # update processed value in portal table
    newSampleIDs = newNovelOmicsSamples['sampleID'].to_list()[0]
    newSubjectIDs = newNovelOmicsSubjects['subjectID'].to_list()[0]
    
    portalShipmentMetadata['processed'] = dt.Frame([
        d in newSampleIDs
        for d in portalShipmentMetadata[:, [f.sample_id]].to_list()[0]
    ])

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~ 
# Process Experiment Manifest
#
# If there is new experiment metadata available, we can process and prepare the
# data for import into RD3. This process does the following.
#
#   1. Validates ERN and organisation codes
#   2. Creates subsets based on experiment type
#   3. Build RD3 tables: samples, labinfo_*, and files
#
# All prepared objects will be imported in the following step.
#

if should_process_experiments:
    status_msg('Processing new experiments...')
    
    # determine new samples and experiments
    newExperiments['isNewSample'] = dt.Frame([
        d not in existingSampleIDs
        for d in newExperiments['sample_id'].to_list()[0]
    ])
    
    newExperiments['isNewExperiment'] = dt.Frame([
        d not in existingExperimentIDs
        for d in newExperiments['project_experiment_dataset_id']
    ])
    
    # recode ERNS and organisations
    newExperiments['ERN'] = dt.Frame([
        recodeErns(d) for d in newExperiments['ERN'].to_list()[0]
    ])

    newExperiments['organisation'] = dt.Frame([
        recodeOrganisations(d)
        for d in newExperiments['organisation'].to_list()[0]
    ])
    
    # process only new experiments
    if newExperiments[f.isNewSample==True, :].nrows == 0:
        Warning('New samples were detected during experiment processing!')
        
    #//////////////////////////////////
    
    # ~ 3a ~
    # Process New Novel Omics WGS Experiments
    
    newWgsData = newExperiments[
        (f.isNewSample==False) & (f.isNewExperiment==True) & (f.experiment_Type=='SR-WGS'), :
    ]
    
    # update experiment and sample IDs
    newWgsData['experimentID'] = dt.Frame([
        d + experimentIdSuffix for d in newWgsData['project_experiment_dataset_id'].to_list()[0]
    ])
    newWgsData['sample'] = dt.Frame([
        d + sampleIdSuffix for d in newWgsData['sample_id'].to_list()[0]
    ])
    
    # ~ 3a.i ~ 
    # Prepare data for (for `rd3_novelomics_labinfo_wgs`)
    newWgsLabInfo = newWgsData[:, first(f[:]), dt.by(f.experiment_id)][:, {
        'id': f.experiment_id,
        'experimentID': f.experiment_id,
        'sample': f.sample_id,
        'capture': f.library_selection,
        'libraryType': f.library_source,
        'library': None,
        'sequencingCenter': f.sequencing_center,
        'sequencer': f.platform_model,
        'seqType': f.library_strategy,
        'patch': release,
        'library_layout': f.library_layout # only needed for processing!
    }]
    
    # recode library values
    newWgsLabInfo['library'] = dt.Frame([
        '1' if d == 'PAIRED' else d
        for d in newWgsLabInfo['library_layout'].to_list()[0]
    ])
    
    del newWgsLabInfo['library_layout']
    
    # format libraryType
    newWgsLabInfo['libraryType'] = dt.Frame([
        d.title() for d in newWgsLabInfo['libraryType'].to_list()[0]
    ])
    
    #//////////////////////////////////
    
    # ~ 3a.ii ~ 
    # Prepare WGS file metadata (for `rd3_novelomics_files`)
    
    newWgsFiles = newWgsData[:,{
        'EGA': f.file_ega_id,
        'file_path': f.file_path, # only needed for processing
        'name': f.file_path,
        'md5': f.unencrypted_md5_checksum,
        'typeFile': f.filt_type,
        'samples': f.sample_id,
        'experimentID': f.experimentID,
        'patch': release,
        # set `dateCreated` (i.e., created in RD3 not file creation date)
        'dateCreated': datetime.today().strftime('%Y-%m-%d')
    }]
    
    # concat filename attribute `name` and remove `file_path`
    newWgsFiles['name'] = dt.Frame([
        d['path'] + '/' + d['name']
        for d in newWgsFiles[:,['file_path','name']].to_tuples()
    ])
    
    # set import flags if applicable
    if newWgsLabInfo.nrow > 0: should_import_labs_wgs = True
    if newWgsFiles.nrow > 0: should_import_files = True
      
#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Identify New Organisations and ERNs
#
# Before importing new subjects, samples, and experiments into RD3, make sure
# all organisations and ERNs exist in RD3. New ERNs are likely to be the result
# of a name variation as there are a fixed number of ERNs. Organisations are
# likely to be new or variations of an existing name.
#

if newSamples.nrows:

    # ~ 4a ~
    # Identify new organisations
    organisations = dt.unique(newSamples[:, f.organisation])
    organisations['isNewOrg'] = dt.Frame([
        d not in organisationsIDs
        for d in organisations['organisation'].to_list()[0]
    ])

    if organisations[f.isNewOrg, :].nrows:
        Warning('New Organisations detected. Manually review these cases before importing')
        should_import_organisations = True

    # ~ 4b ~
    # Identify new ERNs
    erns = dt.unique(newSamples[:, f.ERN])
    erns['isNewErn'] = dt.Frame([
        d not in ernIDs
        for d in erns['ERN'].to_list()[0]
    ])
    
    if erns[f.isNewErn, :].nrows:
        Warning('New ERNs detected. Manually review these cases before importing')
        should_import_erns = True




#//////////////////////////////////////////////////////////////////////////////
 
# ~ 5 ~ 
# Import data
#
# Using the flags defined early on in this script, import the corresponding
# datasets. Make sure reference tables are updated first.


# ~ 5a ~ 
# Import Organisations
if should_import_organisations:
    status_msg('Importing new organisations...')
    newRD3Orgs = (
        organisations[f.isNewOrg, {
            'identifier': f.organisation,
            'name': f.organisation
        }]
        .to_pandas()
        .to_dict('records')
    )

    rd3.update_table(newRD3Orgs, 'rd3_organisation')
else:
    status_msg('No new organisations detected...')

    
# ~ 5b ~
# Import ERNs
if should_import_erns:
    status_msg('Importing new ERNs...')
    newRD3Erns = (
        erns[f.isNewErn, {'identifier': f.ERN}]
        .to_pandas()
        .dict('records')
    )
    rd3.update_table(newRD3Erns, 'rd3_ern')
else:
    status_msg('No new ERNs detected')


# ~ 5c ~ 
# import subjects
if should_import_subjects:
    status_msg('Importing new subjects...')
    
    rd3_subject = newNovelOmicsSubjects.to_pandas().to_dict('records')
    rd3_subjectinfo = newNovelOmicsSubjects[
        :, {
            'id': f.id,
            'subjectID': f.id,
            'patch': f.patch
        }
    ].to_pandas().to_dict('records')
    
    rd3.update_table(rd3_subject, 'rd3_novelomics_subject')
    rd3.update_table(rd3_subjectinfo, 'rd3_novelomics_subjectinfo')

else:
    status_msg('No new subjects to register. :-)')
      


# ~ 5d ~      
# import samples
if should_import_samples:
    status_msg('Imporing new samples...')
    
    rd3_sample = toRecords(newNovelOmicsSamples)
    rd3.update_table(rd3_sample, 'rd3_novelomics_sample')

else:
    status_msg('No new samples to register')


# ~ 5e ~
# import experiments
if should_import_labs_wgs:
    status_msg('Importing new experiments...')
    
    rd3_labinfo_wgs = newWgsLabInfo.to_pandas().to_dict('records')
    rd3.update_table(rd3_labinfo_wgs, 'rd3_novelomics_labinfo_wgs')
    
else:
    status_msg('No new experiments to register')


# ~ 5f ~
# import files
if should_import_files:
    status_msg('Importing new files...')
    
    rd3_file = newWgsFiles.to_pandas().to_dict('records')
    rd3.update_table(rd3_file, 'rd3_novelomics_file')

else:
    status_msg('No new files to register')
    

status_msg('Done!! :-)')
    

# ~ 5e ~ 
# Update processed status in shipment table
rd3_portal_statuses = portalShipmentMetadata[:, [f.molgenis_id, f.processed]]
rd3_portal_statuses = toRecords(rd3_portal_statuses)
rd3.batch_update_one_attr('rd3_portal_novelomics_shipment','processed', rd3_portal_statuses)

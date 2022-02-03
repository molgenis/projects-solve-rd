# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2022-01-21
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

import molgenis.client as molgenis
from datatable import dt,f, first
from urllib.parse import quote_plus
from datetime import datetime
import json
import requests

# set molgenis.client info
host = 'http://localhost/api/'
token = '${molgenisToken}'

# set current release information
release = 'novelomics_original' # ex: 'freezeN_original' or 'freezeN_patchX'
experimentIdSuffix = '_original'
sampleIdSuffix = '_original'

# generic timestamped messages
def status_msg(*args):
    """Status Message
    Prints a message with a timestamp
    @param *args : message to write 
    """
    msg = ' '.join(map(str, args))
    timestamp = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
    print('[{}] {}'.format(timestamp, msg))


# extend the molgenis class
class molgenis(molgenis.Session):
    """molgenis
    An extension of molgenis.client
    """
    def __baseUrl__(self):
        props = list(self.__dict__.keys())
        if '_url' in props: return self._url
        if '_api_url' in props: return self._api_url
    

    def update_table(self, data, entity):
        """Update Table
        When importing data into a new table using the client, there is a 1000
        row limit. This method allows you push data without having to worry
        about the limits.
        
        @param entity (str) : name of the entity to import data into
        @param data (list) : data to import
        
        @return a status message
        """
        url = f'{self.__baseUrl__()}v2/{quote_plus(entity)}'
        # single push
        if len(data) < 1000:
            try:
                response = self._session.post(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities' : data})
                )
                if not response.status_code // 100 == 2:
                    return f'Error: unable to import data({response.status_code}): {response.content}'
                print(f'Imported {len(data)} entities into {str(entity)}')
            except requests.exceptions.HTTPError as err:
                raise SystemError(err)
        # batch push
        if len(data) >= 1000:    
            for d in range(0, len(data), 1000):
                try:
                    response = self._session.post(
                        url = url,
                        headers = self._get_token_header_with_content_type(),
                        data = json.dumps({'entities': data[d:d+1000] })
                    )
                    if not response.status_code // 100 == 2:
                        raise response.raise_for_status()
                    print(f'Batch {d}: Imported {len(data)} entities into {str(entity)}')
                except requests.exceptions.HTTPError as err:
                    raise SystemError(f'Batch {d} Error: unable to import data:\n{str(err)}')


    def batch_update_one_attr(self, entity: str, attr: str, data: list):
        """Batch Update One Attribute
        Import data for an attribute in batches (i.e., into groups of 1000 entities).
        Data should be a list of dictionaries with two keys: `id` and <attr> where
        attr is the name of the attribute that you would like to update
        
        @param data (list) : data to import
        @param attr (str) : name of the attribute to update
        @param entity (str) : name of the entity to import data into
        
        @return a response code
        """
        url = f'{self.__baseUrl__()}v2/{quote_plus(entity)}/{attr}' 
        for d in range(0, len(data), 1000):
            try:
                response = self._session.put(
                    url = url,
                    headers = self._get_token_header_with_content_type(),
                    data = json.dumps({'entities': data[d:d+1000] })
                )
                if not response.status_code // 100 == 2:
                    raise response.raise_for_status()
                print(f'Batch {d}: Imported {len(data)} entities into {str(entity)}')
            except requests.exceptions.HTTPError as err:
                raise SystemError(f'Batch {d} Error: unable to import data:\n{str(err)}')

def recodeErns(value):
    """Recode ERNs
    Recode known ERN name variations into RD3 terminology.
    @param value (str) : value to recode
    @return string
    """
    patterns = {
        'genturis': 'ERN-GENTURIS',
        'ithaca': 'ERN-ITHACA',
        'NMD': 'ERN-NMD',
        'RND': 'ERN-RND'
    }
    
    if value.lower() in patterns:
        return patterns[value]

def recodeOrgs(value):
    """Recode Organisations
    Recode known organisation name variations into RD3 terminology.
    @param value (str) : value to recode
    @return string
    """
    patterns={
        'malgorzata  dec-cwiek': 'malgorzata-dec-cwiek'
    }
    if value.lower() in patterns:
        return patterns[value]

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull Required Data
#
# For the novel omics mapping job, we need to pull all unprocessed novel omics
# data located in the portal (shipment and experiment)

# start new session
rd3 = molgenis(url=host, token=token)

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

del newExperiments['_href']

# pull `rd3_portal_novelomics_shipment` (subjects and samples)
newSamples = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        q='processed==false',
        batch_size=10000
    )
)

del newSamples['_href']

#//////////////////////////////////////

# ~ 1b ~
# Pull existing rd3_novelomics datasets for reference

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
    experiment['sampleKey'] = experiment['sample'].get('id')
    experiment['sampleID'] = experiment['sample'].get('sampleID')
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

del rd3_ern[:,'_href'], rd3_organisation[:,'_href']

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


if newExperiments and newSamples:
    status_msg(
        'Will process {} new experiments and {} samples'
        .format(len(newExperiments), len(newSamples))
    )
    should_process_experiments = True
    should_process_samples = True
elif newExperiments and not newSamples:
    status_msg(
        'Will process {} new experiments and 0 samples'
        .format(len(newExperiments))
    )
    should_process_experiments = True
elif not newExperiments and newSamples:
    status_msg(
        'Will process 0 new experiments and {} samples'
        .format(len(newSamples))
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
        d in existingSubjectIDs
        for d in newSamples['participant_subject'].to_list()[0]
    ])
    
    newSamples['isNewSample'] = dt.Frame([
        d in existingSampleIDs
        for d in newSamples['sample_id'].to_list()[0]
    ])

    newSamples['ERN'] = dt.Frame([recodeErns(d) for d in newSamples['ERN'].to_list()[0]])
    newSamples['organisation'] = dt.Frame([recodeOrgs(d) for d in newSamples['organisation'].to_list()[0]])
    
    #//////////////////////////////////
    
    # ~ 2a ~
    # Prepare data for `rd3_novelomics_subject`
    # New subjects will be added to RD3.

    if newSamples[f.isNewSubject==True, :].nrows > 0:
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
    if newSamples[f.isNewSample==True, :].nrows > 0:
            status_msg('Preparing new samples for registration in RD3...')
            
            newNovelOmicsSamples = newSamples[f.isNewSample==True,:][
                :, first(f[:]), dt.by(f.sample_id)
            ][:, {
                'id': f.sample_id,
                'subject': f.participant_subject,
                'tissueType': f.tissue_type,
                'materialType': f.sample_type,
                'patch': release,
                'batch': f.batch,
                'typeOfAnalysis': f.type_of_analysis,
                'organisation': f.organisation,
                'ERN': f.ERN

            }]
            
            newNovelOmicsSamples['tissueType'] = dt.Frame([
                'Whole Blood' if d in ['whole blood','blood'] else d
                for d in newNovelOmicsSamples['tissueType'].to_list()[0]
            ])
            
            should_import_samples=True

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
        recodeOrgs(d) for d in newExperiments['organisation'].to_list()[0]
    ])
    
    # process only new experiments
    if newExperiments[f.isNewSample==False, :].nrows == 0:
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
# 
# ~ 4 ~ 
# Import data

if should_import_subjects:
    status_msg('Importing new subjects...')
    
    rd3_subject = newNovelOmicsSubjects.to_pandas().to_dict('records')
    rd3_subjectinfo = newNovelOmicsSubjects[
        :, [f.id,f.subjectID, f.patch]
    ].to_pandas().to_dict('records')
    
    rd3.update_table(rd3_subject, 'rd3_novelomics_subject')
    rd3.update_table(rd3_subjectinfo, 'rd3_novelomics_subjectinfo')

else:
    status_msg('No new subjects to register. :-)')
      
if should_import_samples:
    status_msg('Imporing new samples...')
    
    rd3_sample = newNovelOmicsSamples.to_pandas().to_dict('records')
    rd3.update_table(rd3_sample, 'rd3_novelomics_sample')

else:
    status_msg('No new samples to register')


if should_import_labs_wgs:
    status_msg('Importing new experiments...')
    
    rd3_labinfo_wgs = newWgsLabInfo.to_pandas().to_dict('records')
    rd3.update_table(rd3_labinfo_wgs, 'rd3_novelomics_labinfo_wgs')
    
else:
    status_msg('No new experiments to register')

if should_import_files:
    status_msg('Importing new files...')
    
    rd3_file = newWgsFiles.to_pandas().to_dict('records')
    rd3.update_table(rd3_file, 'rd3_novelomics_file')

else:
    status_msg('No new files to register')
    

status_msg('Done!! :-)')
    
    
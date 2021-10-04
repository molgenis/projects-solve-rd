# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2021-09-30
# PURPOSE: process novel omics into Solve RD
# STATUS: working
# DEPENDENCIES: molgenis.client, os, json, datetime, time
# COMMENTS: The purpose of this script is to map novel omics metadata into
# main RD3 tables. Data is uploaded by external partners and this workflow
# maps values into RD3 terminology and imports them into the correct freeze.
# At the moment, this script covers Freeze 1 & 2. If new data does not exist
# in either freeze, then records are pushed into a 'holding' table. This script
# will likely need to be updated to pull and process data from the 'holding'
# table.
# //////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis
from datetime import datetime
from urllib.parse import quote_plus
import json
import requests


def status_msg(*args):
    """Status Message
    
    Prints a message with a timestamp
    
    @param *args : message to write 
    
    """
    msg = ' '.join(map(str, args))
    timestamp = datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]
    print('\033[94m[' + timestamp + '] \033[0m' + msg)   


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# EXTEND THE MOLGENIS CLASS
# Add the following methods for pushing data into RD3
# - update_table: batch update a table
# - batch_update_one_attr: batch update an attribute 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class molgenis(molgenis.Session):
    """molgenis
    
    An extension of the molgenis.client class
    
    """
    
    # update_table
    def update_table(self, data, entity):
        """Update Table
        
        When importing data into a new table using the client, there is a 1000
        row limit. This method allows you push data without having to worry
        about the limits.
        
        @param self required class param
        @param data object containing data to import
        @param entity ID of the target entity written as 'package_entity'
        
        @return a response code
        """
        if len(data) < 1000:
            response = self._session.post(
                url = self._url + 'v2/' + quote_plus(entity),
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities' : data})
            )
            if response.status_code == 201:
                status_msg(
                    'Successfully imported data (response: {})'
                    .format(response.status_code)
                )
            else:
                status_msg(
                    'Failed to import data (response: {}): \nReason:{}'
                    .format(response.status_code, response.content)
                )
        else:    
            for d in range(0, len(data), 1000):
                response = self._session.post(
                    url=self._url + 'v2/' + entity,
                    headers=self._get_token_header_with_content_type(),
                    data=json.dumps({'entities': data[d:d+1000]})
                )
                if response.status_code == 201:
                    status_msg(
                        'Successfuly imported batch {} (response: {})'
                        .format(d, response.status_code)
                    )
                else:
                    status_msg(
                        'Failed to import data (response: {}): \nReason:{}'
                        .format(response.status_code, response.content)
                    )

    # batch_update_one_attr
    def batch_update_one_attr(self, entity, attr, values):
        """Batch Update One Attribute
        
        Import data for an attribute in groups of 1000
        
        @param self required class param
        @param entity ID of the target entity written as `package_entity`
        @param values data to import, a list of dictionaries where each dictionary
              is structured with two keys: the ID attribute and the attribute
              that you wish to update. E.g. [{'id': 'id123", 'x': 1},...]
        
        @return a response code
        """
        add = 'No new data'
        for i in range(0, len(values), 1000):
            add = 'Update did tot go OK'
            """Updates one attribute of a given entity with the given values of the given ids"""
            response = self._session.put(
                self._url + "v2/" + quote_plus(entity) + "/" + attr,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': values[i:i+1000]})
            )
            if response.status_code == 200:
                add = 'Update went OK'
            else:
                try:
                    response.raise_for_status()
                except requests.RequestException as ex:
                    self._raise_exception(ex)
                return response
        return add


def distinct_dict(data: list = None, key: str = None):
    """Distinct Dictionaries
    
    In a list of dictionnaires, return distinct dictionaries by a given key
    
    @param data a list containing one or more dictionaries 
    @param key one or more keys to filter by
    
    @return a list of dictionaries
    """
    if key is None:
        key = lambda x: x
    seen = set()
    for d in data:
        k = key(d)
        if k in seen:
            continue
        yield d
        seen.add(k)
    return seen


def flatten_attr(data: list = None, attr: str = None, distinct: bool = False):
    """Flatten attribute
    
    In a list of dictionaries, pull values by key
    
    @param data list of dictionaries
    @param name of attribute to flatten
    @param distinct if TRUE, return unique cases only
    
    @return a list of values
    """
    out = []
    for d in data:
        tmp_attr = d.get(attr)
        out.append(tmp_attr)
    if distinct:
        return list(set(out))
    else:
        return out


def find_dict(data: list = None, attr: str = None, value: str = None):
    """Filter list of dictionaries
    
    @param data object to search
    @param attr variable find match
    @param value value to filter for
    
    @return a list of a dictionaries

    """
    return list(filter(lambda d: d[attr] in value, data))


def select_keys(data, keys):
    """Select Keys

    Reduce list of dictionaries to named keys
    
    @param data list of dictionaries to select
    @param keys an array of values
    
    @return a list of dictionaries
    """
    return list(map(lambda x: {k: v for k, v in x.items() if k in keys}, data))


def lookups_find_new(lookup, lookup_attr, new):
    """Find new lookup values
    
    Given a list of new values and old values, determine if there are any
    new values.
    
    @param lookup_attr the attribute to look into
    @param lookup RD3 lookup table
    @param new a list of unique values
    
    @return a list of dictionaries of 
    """
    refs = flatten_attr(lookup, lookup_attr)
    out = []
    for n in new:
        if (n in refs) == False:
            out.append({'id': n, 'label': n})
    return out


def lookups_prep_new(data, id_name='identifier', label_name='label', clean = False):
    """Prepare Reference Types for Import
    
    Prepare data for import into Molgenis
    
    @param data list containing one or more dictionaries of new references
    @param id_name label to apply to the ID variable (id => identifier)
    @param label_name label to map to 'label' key (i.e., name => label)
    @param clean if TRUE, the ID attribute will be cleaned (spaces => '-'; text => lowered)
    
    @return a list of dictionaries
    """
    out = []
    for d in data:
        new = {}
        value  = d.get('id')
        label = d.get('label')
        if clean:
            value = '-'.join(d.get('id').split()).lower()
            label = value
        new[id_name] = value
        new[label_name] = label
        out.append(new)
    return out


def map_rd3_subject(data, patch, distinct=True):
    """Map RD3 Subjects

    Using a reference ID list, update patch data for matching subjects

    @param data a list containing 1 or more dictionaries
    @param patch string containing a new patch ID
    @param distinct If True, unique patientis are returned
    
    @return a list of dictionaries
    """
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('participant_subject') + '_' + patch
        tmp['subjectID'] = d.get('participant_subject')
        tmp['patch'] = patch
        tmp['organisation'] = d.get('organisation')
        tmp['ERN'] = d.get('ERN')
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, key=lambda x: x['id'] ))
    return out


def map_rd3_subjectinfo(data):
    """Map RD3 Subject Information

    Create new entity for subjectinfo

    @param data processed subject data (output from `map_rd3_subject`)
    
    @return a list of dictionaries containing relevant subjectinfo data
    """
    out = []
    for d in data:
        out.append({
            'id': d.get('id'),
            'subjectID': d.get('id'),
            'patch': d.get('patch')
        })
    return out


def map_rd3_samples(data, id_suffix, subject_suffix, patch, distinct=False):
    """Map new RD3 samples
    
    @param data input data
    @param id_suffix content to append to ID, e.g., "_original"
    @param subject_suffix string indicating which patch in rd3_freeze*_subject links to
    @param patch SolveRD3 data release
    @param distinct if True, distinct dictionaries will be returned
    
    @return list of dictionaries
    """
    out = []
    for d in data:
        tmp = {
            'id': d.get('sample_id') + id_suffix,
            'subject': d.get('subject_id') + subject_suffix,
            'sampleID': d.get('sample_id'),
            'alternativeIdentifier': d.get('alternativeIdentifier', None),
            'tissueType': d.get('tissue_type'),
            'materialType': d.get('sample_type'),
            'patch': patch,
            'batch': d.get('project_batch_id', None),
            'organisation': d.get('organisation'),
            'ERN': d.get('ERN')
        }
        if tmp['tissueType'] == 'blood':
            tmp['tissueType'] = 'Whole Blood'
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['sampleID'] ) ))
    else:
        return out


def map_rd3_labinfo_wgs(data, id_suffix, sample_id_suffix, patch, distinct=False):
    """Map new RD3 WGS labinfo
    
    @param data input data
    @param id_suffix content to append to ID, e.g., "_original"
    @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
    @param patch SolveRD3 data release
    @param distinct if True, distinct dictionaries will be returned
    
    @return list of dictionaries
    """
    out = []
    for d in data:
        tmp = {
            'id': d.get('project_experiment_dataset_id') + id_suffix,
            'experimentID': d.get('project_experiment_dataset_id'),
            'sample': d.get('sample_id') + sample_id_suffix,
            'capture': d.get('library_selection'),
            'libraryType': d.get('library_source').title(),
            'library': None,
            'sequencingCentre': d.get('sequencing_center'),
            'sequencer': d.get('platform_model'),
            'seqType': d.get('library_strategy'),
            'patch': patch
        }
        if d.get('library_layout') == 'PAIRED':
            tmp['library'] = '1'
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['experimentID'] )))
    else:
        return out


def map_rd3_labinfo_rnaseq(data, id_suffix, sample_id_suffix, patch, distinct = False):
    """Map new RD3 RNAseq labinfo
    
    @param data input data
    @param id_suffix content to append to ID, e.g., "_original"
    @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
    @param patch SolveRD3 data release
    @param distinct if True, distinct dictionaries will be returned
    
    @return list of dictionaries
    """
    out = []
    for d in data:
        tmp = {
            'id': d.get('project_experiment_dataset_id') + id_suffix,
            'experimentID': d.get('project_experiment_dataset_id'),
            'sample': d.get('sample_id') + sample_id_suffix,
            'patch': patch
        }
        
        out.append(tmp)
    if distinct:
        return list(distinct_dict(out, lambda x: ( x['id'], x['experimentID'] )))
    else:
        return out


def map_rd3_files(data, sample_id_suffix, patch):
    """Map new freeze files

    @param data input data
    @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
    @param patch SolveRD3 data release

    @return list of dictionaries
    """
    out = []
    for d in data:
        tmp = {}
        tmp['EGA'] = d.get('file_ega_id')
        tmp['name'] = d.get('file_path') + '/' + d.get('file_name')
        tmp['md5'] = d.get('unencrypted_md5_checksum')
        tmp['typeFile'] = d.get('file_type')
        tmp['samples'] = d.get('sample_id') + sample_id_suffix
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['patch'] = patch
        tmp['dateCreated'] = datetime.today().strftime('%Y-%m-%d')
        out.append(tmp)
    return out


def merge_affiliation_data(x, y):
    """Merge Afilliation Data

    Merge ERN and organisation metadata with main dataset

    @param x primary dataset in which to merge data to (experiment metadata)
    @param y secondary dataset to merge with the main dataset (shipment)

    @return list of dictionaries
    """
    out = []
    for dat in x:
        tmp = dat
        tmp['organisation'] = None
        tmp['ERN'] = None
        result = find_dict(y, 'participant_subject', dat['subject_id'])
        if result:
            tmp['organisation'] = result[0].get('organisation')
            tmp['ERN'] = result[0].get('ERN')
        out.append(tmp)
    return out


def recode_erns(data, refs, attr = 'ERN'):
    """Recode ERNS
    Recode raw values in the ERN column to RD3 terminology

    @param data input data
    @param refs a list containing unique ERN values
    @param attr name of the key to validate

    @returns string
    """
    patterns = {
        'ERN-GENTURIS': ['Genturis', 'GENTURIS', 'genturis'],
        'ERN-ITHACA': ['Ithaca', 'ithaca'],
        'ERN-NMD': ['NMD'],
        'ERN-RND': ['RND']
    }
    out = []
    for d in data:
        tmp = d
        if not(tmp[attr] in refs):
            for pattern in patterns:
                if tmp[attr] in patterns[pattern]:
                    tmp[attr] = pattern
                else:
                    raise ValueError(
                        'Error in recode_erns: {} does not exist'
                        .format(tmp[attr])
                    )
        out.append(tmp)
    return out


def recode_orgs(data, refs, attr = "organisation"):
    """Recode Organisations
    Recode raw values in the organisation column to RD3 terminology
    
    @param data input data
    @param refs a reference list containing unique organisation codes
    @param attr name of the key to validate
    
    @return list of dictionaries
    """
    patterns = {
        'malgorzata-dec-cwiek': ['Malgorzata  Dec-Cwiek']
    }
    out = []
    for d in data:
        tmp = d
        if not(tmp[attr] in refs):
            for pattern in patterns:
                if tmp[attr] in patterns[pattern]:
                    tmp[attr] = pattern
                else:
                    raise ValueError(
                        'Error in recode_orgs: {} does not exist'
                        .format(tmp[attr])
                    )
        out.append(tmp)
    return out


def update_portal_experiment_tbl(data):
    """Update Portal Experiment Data
    
    Set the value of processed Freeze1 and Freeze2 data to True
    
    @param data input dataset containing a list of dictionaries
    
    @return string containing data request response
    """
    update = []
    for d in data:
        update.append({'molgenis_id': d.get('molgenis_id'), 'processed': True})
    rd3.batch_update_one_attr(
        entity='rd3_portal_novelomics_experiment',
        attr = 'processed',
        values = update
    )


def update_portal_shipment_tbl(metadata):
    """Update Portal Shipment Staging Table
    
    For Freeze1- & 2 cases, update the processed attribute to True
    
    @param metadata list containing shipmeny metadata
    
    @return a response code
    """
    records = select_keys(
        data = metadata,
        keys = ['molgenis_id']
    )
    update = []
    for r in records:
        update.append({'molgenis_id': r.get('molgenis_id'), 'processed': True})
    rd3.batch_update_one_attr(
        entity='rd3_portal_novelomics_shipment',
        attr='processed',
        values=update
    )


def triage_ids(data, attr, freeze1, freeze2):
    """Triage Release IDs

    Determine if IDs exist in any other release

    @param data dataset to evalue
    @param attr attribute to search
    @param freeze1 freeze1 reference data (list of values)
    @param freeze2 freeze2 reference data (list of values)
    
    @return a dictionary of lists of IDs by freeze
    """
    out = {'freeze1': [], 'freeze2': []}
    for d in data:
            if d.get(attr) in freeze1:
                if not(d.get(attr) in out['freeze1']):
                    out['freeze1'].append(d.get(attr))
            if d.get(attr) in freeze2:
                if not(d.get(attr) in out['freeze2']):
                    out['freeze2'].append(d.get(attr))
    return out


def process_freeze_subject_ids(data, refIDs, patch):
    """Process freeze subject IDs
    
    Description For IDs that have been identified in novelomics, locate them
    in the freeze datasets, extract, and prep for import.

    @param data freeze dataset
    @param refIDs a list of IDs (a nested listed from `triage_ids`)
    @param patch the current patch

    @return a list of dictionaries
    """
    out = []
    for d in data:
        if d['subjectID'] in refIDs:
            tmp = {
                'id': d.get('id'),
                'patch': d.get('patch')
            }
            if len(tmp['patch']) >= 1:
                tmp_patches = flatten_attr(data = tmp['patch'], attr = 'id')
                if not(patch in tmp_patches):
                    tmp['patch'] = ','.join(map(str, tmp_patches)) + ',' + patch
                else:
                    tmp['patch'] = ','.join(map(str, tmp_patches))
            else:
                tmp['patch'] = patch
            out.append(tmp)
    return out


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Configure Molgenis Session
# When using in production, make sure token is declared using '{molgenisToken}'
# and the host is set to 'http://localhost/api/'.
#
# If running locally, use the `login` method and manually set the host url.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
status_msg('Starting new molgenis session...')
rd3 = molgenis(url = 'http://localhost/api/', token = '${molgenisToken}')

# for local use
# rd3 = molgenis(url = 'https://solve-rd-acc.gcc.rug.nl/api/')
# rd3.login('', '') # for local dev

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# FETCH PORTAL DATA
#
# Not only do we need to pull the novelomics metadata, we also need to pull
# data from the main RD3 tables and several reference entities. The subsequent
# steps will do the following.
#
# 1. Identify cases that exist in the novelomics release and in the freezes.
#   (We will need to update the patch information for these cases)
# 2. Validate ERN codes
# 3. Validate Organisation codes
# 4. Pull metadata from the subject tables in order to map to other tables
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
status_msg('Fetching required data from RD3')

# pull `rd3_portal_novelomics_shipment`
all_metadata = rd3.get('rd3_portal_novelomics_shipment', batch_size = 10000)
metadata = list(filter(lambda x: x['processed'], all_metadata))


# pull `rd3_portal_novelomics_experiment`
experiment = rd3.get(
    entity = 'rd3_portal_novelomics_experiment',
    q='processed==false',
    batch_size=10000,
)

# pull `rd3_novelomics_subject` and flatten IDs
rd3_novelomics_subject = rd3.get(
    entity = 'rd3_novelomics_subject',
    attributes = 'id,subjectID,patch,ERN,organisation',
    batch_size = 10000
)

existing_novelomics_subjects = flatten_attr(rd3_novelomics_subject, 'subjectID')


# pull `rd3_<freeze>_subject`
rd3_subjects = {
    'freeze1': rd3.get(
        entity = 'rd3_freeze1_subject',
        attributes = 'id,subjectID,patch',
        batch_size = 10000
    ),
    'freeze2': rd3.get(
        entity = 'rd3_freeze2_subject',
        attributes = 'id,subjectID,patch',
        batch_size = 10000
    )    
}

# pull `rd3_organisation` and `rd3_ERN` and flatten
rd3_ERN = rd3.get('rd3_ERN')
rd3_organisations = rd3.get('rd3_organisation')
ern_refs = flatten_attr(data = rd3_ERN, attr = 'identifier')
org_refs = flatten_attr(data = rd3_organisations, attr = 'identifier')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# DETERMINE IF MAPPING IS NECESSARY
#
# Since this script is setup as a regular job, there may not always be new
# data to process. This script uses a few flags to determine which dataset
# needs to be processed and imported.
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
should_map_expr = False
should_map_meta = False
should_import_novelomics_subject = False
should_import_novelomics_experiment = False

if experiment and metadata:
    status_msg('New experiment and sample metadata available for processing')
    should_map_expr = True
    should_map_meta = True
elif experiment and not metadata:
    status_msg('New experiment metadata is available for processing')
    should_map_expr = True
elif not experiment and metadata:
    status_msg('New sample metadata is available for processing')
    should_map_meta = True
else:
    status_msg('Everything is up to date :-)')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# ~ 1 ~
# PROCESS PATIENT METADATA
#
# The processing and importing of patient metadata were seperated. It was
# reported that the sample IDs and experiment IDs in the sample manifest
# were not always correct. This could be due to a number of reasons. Rather
# than creating extensive mappings, it was decided to process patients
# independent of samples and experiment metadata.
#
# If data is available to map, prepare for import into `rd3_novelomics`.
# This step works in the following steps.
#
# 1. Pull existing novelomics subjects to determine if there are new subjects
# 2. New cases are checked for matching entries in other releases (freezes)
# 3. The patch information is updated for any matches
# 4. New cases are mapped to RD3 terminology
# 
# Data is imported into molgenis in the next step and if the import flags are
# set to True.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if should_map_meta:
    status_msg('Starting sample metadata processing...')    
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # IDENTIFY NEW PARTICIPANTS
    # First, we need to determine if the subject is a new to the novelomics release
    # or if they already exist. We do not need to reregister if this is case.
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    status_msg('Finding new records...')
    for subject in metadata:
        if subject['participant_subject'] in existing_novelomics_subjects:
            subject['exists'] = 'yes'
        else:
            subject['exists'] = 'no'

    # create subsets
    new_metadata = find_dict(data = metadata, attr = 'exists', value = 'no')
    old_metadata = find_dict(data = metadata, attr = 'exists', value = 'yes')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESS NEW METADATA
    # For new participants, we can "register" them in `rd3_novelomics_subject`.
    # In addition, there are a few other things we need to do.
    #
    # 1. Determine if they exist in another freeze (freeze1, freeze2, etc.)
    # 2. Validate ERN and Organistion codes
    # 3. Build `rd3_novelomics_subject` and `rd3_novelomics_subjectinfo`
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    if new_metadata:
        status_msg('New subjects detected...')

        # create a list of IDs for all current releases
        new_metadata_ids = flatten_attr(new_metadata, 'participant_subject')
        freeze1_subject_ids = flatten_attr(rd3_subjects['freeze1'], 'subjectID')
        freeze2_subject_ids = flatten_attr(rd3_subjects['freeze2'], 'subjectID')
        
        # triage novelomics participant IDs - Do they exist in another release?
        # For new releases, update the function `triage_ids`
        status_msg('Triaging IDs: searching for IDs that exist in other RD3 releases')
        subject_freeze_matches = triage_ids(
            data = metadata,
            attr = 'subject_id',
            freeze1 = freeze1_subject_ids,
            freeze2 = freeze2_subject_ids
        )
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # For all freeze matches, pull the patch information and add current
        # novelomics release information
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        for freeze in subject_freeze_matches:
            if subject_freeze_matches[freeze]:
                status_msg(
                    'In {}: identified {} IDs. Will update patch information...'
                    .format(freeze, len(subject_freeze_matches[freeze]))
                )
                
                # prep data with new patch info
                subject_freeze_updates = process_freeze_subject_ids(
                    data = rd3_subjects[freeze],
                    refIDs = subject_freeze_matches[freeze],
                    patch = 'novelomics_original'
                )
                
                # Are there any matches?
                if subject_freeze_updates:
                    entityBasename = 'rd3_' + freeze + '_subject'
                    
                    # update `rd3_<freeze>_subject`
                    rd3.batch_update_one_attr(
                        entity = entityBasename,
                        attr = 'patch',
                        values = subject_freeze_updates
                    )
                    
                    # update `rd3_<freeze>_subjectinfo`
                    rd3.batch_update_one_attr(
                        entity = entityBasename + 'info',
                        attr = 'patch',
                        values = subject_freeze_updates
                    )
                else:
                    status_msg('Updating patch information is not needed')
                        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # VALIDATE ERNS AND ORGANISATIONS
        #
        # Before we can import data, we need to make sure ERN and Organisation codes
        # are valid. If a code does not exist any of the RD3 reference tables
        # an error will be thrown.
        # 
        # If this happens, you will need to determine if...
        #   1) the code entered correctly or if it closely resembles an existing code
        #   2) the code is new
        # 
        # If the first item is the case, add the variation to the list of patterns
        # in the relevant function or create a new pattern.
        #
        # If the code is new, add a new entry in the relevant RD3 lookup table.
        # New ERN codes shouldn't be the case as there is a set number of ERNs.
        # Organisation codes are more likely to be new.
        #
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        status_msg('Validating ERN and Organisation codes')
        experiment = recode_erns(new_metadata, ern_refs, 'ERN')
        experiment = recode_orgs(new_metadata, org_refs, 'organisation')
        
        
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # BUILD NOVELOMICS SUBJECT TABLES
        # Most of the metadata will come from the PED and Phenopacket files, but
        # we will "register" new participants here. These tables are:
        # - `rd3_novelomics_subject`
        # - `rd3_novelomics_subjectinfo`
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        novelomics_subject = map_rd3_subject(data = new_metadata, patch = 'novelomics_original')
        novelomics_subjectinfo = map_rd3_subjectinfo(data = novelomics_subject)

        # update flags: set import status
        should_import_novelomics_subject = True
        status_msg('Mapped NovelOmics Subjects: {}'.format(len(novelomics_subject)))

    else:
        status_msg('No new subjects to register')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# PROCESS EXPERIMENT METADATA
#
# If there is new experiment metadata available, we can process and prepare the
# data for import into RD3. This process does the following.
#
# 1. Merges patient metadata that are only available in the shipment manifest
# 2. Validates ERN and organisation codes
# 3. Creates subsets based on experiment type
# 4. Build RD3 tables: samples, labinfo_*, and files
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if should_map_expr:
    status_msg('Processing experiment metadata...')
    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # PROCESS NEW EXPERIMENT DATA
    #
    # For new experiment metadata, we need to find and merge some subject and
    # sample attributes to the labinfo tables. This information can be found
    # in the shipment metadata data.
    #
    # The attributes that we need are:
    #   - alternativeIdentifier: this is `CNAG_barcode` (RNAseq data only)
    #   - ERN: associated ERN
    #   - organisation: submitting institution 
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    for e in experiment:

        # Check in existing participants
        if e['subject_id'] in existing_novelomics_subjects:
            result = find_dict(
                data = rd3_novelomics_subject,
                attr = 'subjectID',
                value = e['subject_id']
            )[0]
            
        # was participant metadata processed above?
        try:
            new_metadata_ids
        except NameError:
            status = False
        else:
            status = True

        # Check in the new dataset (processed above)
        if status:
            if e['subject_id'] in new_metadata_ids:
                result = find_dict(
                    data = new_metadata,
                    attr = 'participant_subject',
                    value = e['subject_id']
                )[0]
                
        # merge attributes
        barcode = find_dict(all_metadata, 'sample_id', e['sample_id'])
        e['alternativeIdentifier'] = barcode[0].get('CNAG_barcode', None)
        e['ERN'] = result.get('ERN', {}).get('identifier')
        e['organisation'] = result.get('organisation',{}).get('identifier')
        
            
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # VALIDATE ERNS AND ORGANISATIONS
    #
    # Before we can import data, we need to make sure ERN and Organisation codes
    # are valid. If a code does not exist any of the RD3 reference tables
    # an error will be thrown.
    # 
    # If this happens, you will need to determine if...
    #   1) the code entered correctly or if it closely resembles an existing code
    #   2) the code is new
    # 
    # If the first item is the case, add the variation to the list of patterns
    # in the relevant function or create a new pattern.
    #
    # If the code is new, add a new entry in the relevant RD3 lookup table.
    # New ERN codes shouldn't be the case as there is a set number of ERNs.
    # Organisation codes are more likely to be new.
    #
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    status_msg('Validating ERN and Organisation codes')
    experiment = recode_erns(experiment, ern_refs, 'ERN')
    experiment = recode_orgs(experiment, org_refs, 'organisation')

    
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # BUILD RD3 TABLES
    #
    # At this point, we can start mapping the data into the proper RD3
    # structure (samples, labinfo, files, etc.). However, we need to create
    # subsets of the data based on seqType. In RD3, we have created tables
    # for WGS experiment data and RNAseq data. The data requirements are much
    # different and it was decided to split these tables. The rest of the
    # tables can be processed as normal.
    # 
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    status_msg('Processing new data...')
    
    # create subsets based on experiment type
    experiment_wgs = find_dict(experiment, 'experiment_type', 'SR-WGS')
    experiment_rnaseq = find_dict(experiment, 'experiment_type', 'RNA-seq')
    
    # build `rd3_novelomics_samples`
    novelomics_sample = map_rd3_samples(
        data = experiment,
        id_suffix='_novelomics_original',
        subject_suffix="_novelomics_original",
        patch='novelomics_original',
        distinct = True
    )
    
    # build `rd3_novelomics_labinfo_wgs` (if available)
    if experiment_wgs:
        novelomics_labinfo_wgs = map_rd3_labinfo_wgs(
            data = experiment_wgs,
            id_suffix = '_novelomics_original',
            sample_id_suffix = '_novelomics_original',
            patch = 'novelomics_original',
            distinct = True
        )
        status_msg(
            'Mapped NovelOmics Labinfo (WGS): {}'
            .format(len(novelomics_labinfo_wgs))
        )
    
    # build `rd3_novelomics_labinfo_rnaseq` (if available)
    if experiment_rnaseq:
        novelomics_labinfo_rnaseq = []
        status_msg(
            'Mapped NovelOmics Labinfo (RNAseq): {}'
            .format(len(novelomics_labinfo_rnaseq))
        )

    # build `rd3_novelomics_file`
    novelomics_file = map_rd3_files(
        data = experiment,
        sample_id_suffix = "_novelomics_original",
        patch = 'novelomics_original'
    )
    
    # Update flags and print summaries
    should_import_novelomics = True
    status_msg('Mapped NovelOmics Samples: {}'.format(len(novelomics_sample)))
    status_msg('Mapped NovelOmics Files: {}'.format(len(novelomics_file)))

else:
    status_msg('No experiment metadata available for processing :)')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# IMPORT DATA
#
# Based on the flags and the objects that were created, we can import new
# data into the main RD3 Novel omics tables. Subject metadata is processed
# and imported separately given the discrepancies with the identifiers.
#
# Using the import flags, the processed datasets will be imported into the
# corresponding RD3 novelomics table and the processed status in the portal
# will be set to `True`.
#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# import subjects
if should_import_novelomics_subject:
    status_msg('Importing novel omics metadata...')
    
    # import into RD3 tables
    rd3.update_table(novelomics_subject, 'rd3_novelomics_subject')
    rd3.update_table(novelomics_subjectinfo, 'rd3_novelomics_subjectinfo')
    
    # update portal status
    update_portal_shipment_tbl(metadata = new_metadata)
    update_portal_shipment_tbl(metadata = old_metadata)
    status_msg('Done!')
else:
    status_msg('No new subject metadata to import')


# import samples, labinfo_*, and files
if should_import_novelomics_experiment:
    status_msg('Importing novel omics metadata...')
    
    # import into RD3 Novelomics tables
    rd3.update_table(novelomics_sample, 'rd3_novelomics_sample')
    rd3.update_table(novelomics_file, 'rd3_novelomics_file')
    
    # import labinfo tables where applicable
    if novelomics_labinfo_wgs:
        rd3.update_table(novelomics_labinfo_wgs,'rd3_novelomics_labinfo_wgs')

    if novelomics_labinfo_rnaseq:
        rd3.update_table(novelomics_labinfo_rnaseq, 'rd3_novelomics_labinfo_rnaseq')
    
    # update portal status
    update_portal_experiment_tbl(data = experiment)
    status_msg('Done!')

else:
    status_msg('No new experiment metadata to import')

# FIN!
status_msg('Done!! :-)')
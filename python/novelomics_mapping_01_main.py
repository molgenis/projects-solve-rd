# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2021-06-24
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

from datetime import datetime
import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')

# @title map RD3 Subjects
# @description Using a reference ID list, update patch data for matching subjects
# @param data a list containing 1 or more dictionaries
# @param patch string containing a new patch ID
# @param distinct If True, unique patientis are returned
# @return a list containing one or more dictionaries
def map_rd3_subject(data, patch, distinct=True):
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
        return list(rd3tools.distinct_dict(out, key=lambda x: x['id'] ))
    return out


# @title Map RD3 Subject Information
# @description create entity for subjectinfo
# @param data processed subject data (output from `map_rd3_subject`)
# @return a list of dictionaries containing relevant subjectinfo data
def map_rd3_subjectinfo(data):
    out = []
    for d in data:
        out.append({
            'id': d.get('id'),
            'subjectID': d.get('id'),
            'patch': d.get('patch')
        })
    return out

# @title map new rd3 samples
# @param data input data
# @param id_suffix content to append to ID, e.g., "_original"
# @param subject_suffix A string indicating which patch in rd3_freeze*_subject to link to
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_samples(data, id_suffix, subject_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('sample_id') + id_suffix
        tmp['subject'] = d.get('subject_id') + subject_suffix
        tmp['sampleID'] = d.get('sample_id')
        tmp['tissueType'] = d.get('tissue_type')
        if tmp['tissueType'] == 'blood':
            tmp['tissueType'] = 'Whole Blood'
        tmp['materialType'] = d.get('sample_type')
        tmp['patch'] = patch
        tmp['organisation'] = d.get('organisation')
        tmp['ERN'] = d.get('ERN')
        out.append(tmp)
    if distinct:
        return list(rd3tools.distinct_dict(out, lambda x: ( x['id'], x['sampleID'] ) ))
    else:
        return out


# @title map new labinfo
# @param data input data
# @param id_suffix content to append to ID, e.g., "_original"
# @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
# @param patch SolveRD3 data release
# @param distinct if True, distinct dictionaries will be returned
# @return list of dictionaries
def map_rd3_labinfo(data, id_suffix, sample_id_suffix, patch, distinct=False):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('project_experiment_dataset_id') + id_suffix
        tmp['experimentID'] = d.get('project_experiment_dataset_id')
        tmp['sample'] = d.get('sample_id') + sample_id_suffix
        tmp['capture'] = d.get('library_selection')
        tmp['libraryType'] = d.get('library_source').title()
        tmp['library'] = None
        if d.get('library_layout') == 'PAIRED':
            tmp['library'] = '1'
        tmp['sequencingCentre'] = d.get('sequencing_center')
        tmp['sequencer'] = d.get('platform_model')
        tmp['seqType'] = d.get('library_strategy')
        tmp['patch'] = patch
        out.append(tmp)
    if distinct:
        return list(rd3tools.distinct_dict(out, lambda x: ( x['id'], x['experimentID'] )))
    else:
        return out


# @title map new freeze files
# @description map file metadata to target EMX format
# @param data input data
# @param sample_id_suffix string to append to sample ID so that mrefs are properly linked
# @param patch SolveRD3 data release
# @return list of dictionaries
def map_rd3_files(data, sample_id_suffix, patch):
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


# @title Merge Afilliation Data
# @description merge ERN and organisation metadata with main dataset
# @param x primary dataset in which to merge data to (experiment metadata)
# @param y secondary dataset to merge with the main dataset (shipment)
# @return list of dictionaries
def merge_affiliation_data(x, y):
    out = []
    for dat in x:
        tmp = dat
        tmp['organisation'] = None
        tmp['ERN'] = None
        result = rd3tools.find_dict(y, 'participant_subject', dat['subject_id'])
        if result:
            tmp['organisation'] = result[0].get('organisation')
            tmp['ERN'] = result[0].get('ERN')
        out.append(tmp)
    return out


# @title Recode ERNS
# @description recode raw values in the ERN column to RD3 terminology
# @param ern_ref a list containing unique ERN values
# @param value a string containing an ERN code
# @returns a string
def recode_erns(data, refs, attr = 'ERN'):
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
        out.append(tmp)
    return out


# @title Update Portal Experiment Data
# @description Set the value of processed Freeze1 and Freeze2 data to True
# @param data input dataset containing a list of dictionaries
# @return string containing data request response
def update_portal_experiment_tbl(data):
    update = []
    for d in data:
        update.append({'molgenis_id': d.get('molgenis_id'), 'processed': True})
    rd3.batch_update_one_attr(
        entity='rd3_portal_novelomics_experiment',
        attr = 'processed',
        values = update
    )


# @title Update Portal Shipment Staging Table
# @description For Freeze1- & 2 cases, update the processed attribute to True
# @param metadata list containing shipmeny metadata
# @return a response
def update_portal_shipment_tbl(metadata):
    records = rd3tools.select_keys(
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


# @title triage_ids
# @description determine if values exist in reference entities
# @param data dataset to evalue
# @param attr attribute to search
# @param freeze1 freeze1 reference data (list of values)
# @param freeze2 freeze2 reference data (list of values)
def triage_ids(data, attr, freeze1, freeze2):
    out = {'freeze1': [], 'freeze2': []}
    for d in data:
            if d.get(attr) in freeze1:
                if not(d.get(attr) in out['freeze1']):
                    out['freeze1'].append(d.get(attr))
            if d.get(attr) in freeze2:
                if not(d.get(attr) in out['freeze2']):
                    out['freeze2'].append(d.get(attr))
    return out


# @title process freeze subject IDs
# @description For IDs that have been identified in novelomics,
#       locate them in the freeze datasets, extract, and prep
#       for import
# @param data freeze dataset
# @param refIDs a list of IDs (a nested listed from `triage_ids`)
# @param patch the current patch
# @return a list of dictionaries
def process_freeze_subject_ids(data, refIDs, patch):
    out = []
    for d in data:
        if d['subjectID'] in refIDs:
            tmp = {
                'id': d.get('id'),
                'patch': d.get('patch')
            }
            if len(tmp['patch']) >= 1:
                tmp_patches = rd3tools.flatten_attr(data = tmp['patch'], attr = 'id')
                if not(patch in tmp_patches):
                    tmp['patch'] = ','.join(map(str, tmp_patches)) + ',' + patch
                else:
                    tmp['patch'] = ','.join(map(str, tmp_patches))
            else:
                tmp['patch'] = patch
            out.append(tmp)
    return out


#//////////////////////////////////////////////////////////////////////////////

# init session
rd3tools.status_msg('Processing NovelOmics Data...')
rd3 = rd3tools.molgenis(
    url=config['hosts'][config['run']['env']],
    token=config['tokens'][config['run']['env']]
)

# fetch all reference data
rd3tools.status_msg('Fetching RD3 reference entities')
rd3_organisations = rd3.get('rd3_organisation')
rd3_ERN = rd3.get('rd3_ERN')
ern_refs = rd3tools.flatten_attr(data = rd3_ERN, attr = 'identifier')

#//////////////////////////////////////

# fetch raw novelomics data from the portal 
rd3tools.status_msg('Fetching data from the portal')

metadata = rd3.get(
    'rd3_portal_novelomics_shipment',
    q = 'processed==false',
    batch_size = 10000
)

experiment = rd3.get(
    entity = 'rd3_portal_novelomics_experiment',
    q='processed==false',
    batch_size=10000,
)

# find existing novelomics metadata
rd3_novelomics_subject = rd3.get(
    entity = 'rd3_novelomics_subject',
    attributes = 'id,subjectID,patch,ERN,organisation',
    batch_size = 10000
)
existing_novelomics_subjects = rd3tools.flatten_attr(
    data = rd3_novelomics_subject,
    attr = 'subjectID'
)


# determine if mapping is necessary
should_map_expr = False
should_map_meta = False
if experiment and metadata:
    rd3tools.status_msg('New experiment and sample metadata available for processing')
    should_map_expr = True
    should_map_meta = True
elif experiment and not metadata:
    rd3tools.status_msg('New experiment metadata is available for processing')
    should_map_expr = True
elif not experiment and metadata:
    rd3tools.status_msg('New sample metadata is available for processing')
    should_map_meta = True
else:
    rd3tools.status_msg('Everything is up to date :-)')

#//////////////////////////////////////

# ~ 1 ~
# Process Sample+Patient Metadata

should_import_novelomics_subject = False
should_import_novelomics_experiment = False

# If data is available to map, prepare for import into `rd3_novelomics`.
# This step works in the following steps
#
# 1. Pulls existing novelomics subjects to determine if there are new subjects
# 2. New cases are checked for matching entries in other releases (freezes)
# 3. The patch metadata for matches is updated
# 4. New cases are mapped to RD3 terminology (i.e., `rd3_novelomics_subject`)
# 5. Data is imported into molgenis in the next step
#

if should_map_meta:
    rd3tools.status_msg('Starting sample metadata processing...')    
    rd3tools.status_msg('Finding new records...')
    for subject in metadata:
        if subject['participant_subject'] in existing_novelomics_subjects:
            subject['exists'] = 'yes'
        else:
            subject['exists'] = 'no'
    new_metadata = rd3tools.find_dict(
        data = metadata,
        attr = 'exists',
        value = 'no'
    )
    old_metadata = rd3tools.find_dict(
        data = metadata,
        attr = 'exists',
        value = 'yes'
    )
    #//////////////////////////////////
    # Run processing steps if there is new metadata
    if new_metadata:
        rd3tools.status_msg('New subjects detected...')
        rd3tools.status_msg('Fetching Freeze Subject metadata...')
        new_metadata_ids = rd3tools.flatten_attr(
            data = new_metadata,
            attr = 'participant_subject'
        )
        freeze1_subjects = rd3.get(
            entity = 'rd3_freeze1_subject',
            attributes = 'id,subjectID,patch',
            batch_size = 10000
        )
        freeze2_subjects = rd3.get(
            entity = 'rd3_freeze2_subject',
            attributes = 'id,subjectID,patch',
            batch_size = 10000
        )
        # flatten subject IDs
        freeze1_subject_ids = rd3tools.flatten_attr(freeze1_subjects, 'subjectID')
        freeze2_subject_ids = rd3tools.flatten_attr(freeze2_subjects, 'subjectID')
        # triage freeze data
        rd3tools.status_msg('Searching for overlapping freeze IDs...')
        subject_freeze_matches = triage_ids(
            data = metadata,
            attr = 'subject_id',
            freeze1 = freeze1_subject_ids,
            freeze2 = freeze2_subject_ids
        )
        # Update patch info `rd3_freeze1_subject` and `rd3_freeze1_subjectinfo`
        if subject_freeze_matches['freeze1']:
            rd3tools.status_msg(
                'Identified {} Freeze1 IDs. Processing...'
                .format(len(subject_freeze_matches['freeze1']))
            )
            subject_freeze1_updates = process_freeze_subject_ids(
                data = freeze1_subjects,
                refIDs = subject_freeze_matches['freeze1'],
                patch = 'novelomics_original'
            )
            if subject_freeze1_updates:
                rd3tools.status_msg('Updating Freeze1 subject patch info...')
                rd3.batch_update_one_attr(
                    entity = 'rd3_freeze1_subject',
                    attr = 'patch',
                    values = subject_freeze1_updates
                )
                rd3.batch_update_one_attr(
                    entity = 'rd3_freeze1_subjectinfo',
                    attr = 'patch',
                    values = subject_freeze1_updates
                )
            else:
                rd3tools.status_msg('No updates found')
        # Update patch info `rd3_freeze2_subject` and `rd3_freeze2_subjectinfo`
        if subject_freeze_matches['freeze2']:
            rd3tools.status_msg('Identified Freeze2 IDs. Processing...')
            subject_freeze2_updates = process_freeze_subject_ids(
                data = freeze1_subjects,
                refIDs = subject_freeze_matches['freeze2'],
                patch = 'novelomics_original'
            )
            if subject_freeze2_updates:
                rd3tools.status_msg('Updating Freeze2 Subject patch info...')
                rd3.batch_update_one_attr(
                    entity = 'rd3_freeze2_subject',
                    attr = 'patch',
                    values = subject_freeze2_updates
                )
                rd3.batch_update_one_attr(
                    entity = 'rd3_freeze2_subjectinfo',
                    attr = 'patch',
                    values = subject_freeze2_updates
                )
            else:
                rd3tools.status_msg('No updates found')
        #//////////////////////////////
        # mapping subject information
        rd3tools.status_msg('Processing ERN data...')
        new_metadata = recode_erns(
            data = new_metadata,
            refs = ern_refs,
            attr = 'ERN'
        )
        rd3tools.status_msg('Manually fixing organisations...')
        for n in new_metadata:
            if n['organisation'] == 'Malgorzata  Dec-Cwiek':
                n['organisation'] = 'malgorzata-dec-cwiek'
        novelomics_subject = map_rd3_subject(
            data = new_metadata,
            patch = 'novelomics_original'
        )
        novelomics_subjectinfo = map_rd3_subjectinfo(data = novelomics_subject)
        rd3tools.status_msg('Mapped NovelOmics Subjects: {}'.format(len(novelomics_subject)))
        should_import_novelomics_subject = True
    else:
        rd3tools.status_msg('No new subjects to register')

#//////////////////////////////////////

# ~ 2 ~
# Process Experiment Metadata


if should_map_expr:
    rd3tools.status_msg('Processing experiment metadata...')
    for e in experiment:
        if e['subject_id'] in existing_novelomics_subjects:
            result = rd3tools.find_dict(
                data = rd3_novelomics_subject,
                attr = 'subjectID',
                value = e['subject_id']
            )[0]
        if new_metadata_ids:
            if e['subject_id'] in new_metadata_ids:
                result = rd3tools.find_dict(
                    data = new_metadata,
                    attr = 'participant_subject',
                    value = e['subject_id']
                )[0]
        e['ERN'] = result.get('ERN', {}).get('identifier')
        e['organisation'] = result.get('organisation',{}).get('identifier')
    experiment = recode_erns(
        data = experiment,
        refs = ern_refs,
        attr = 'ERN'
    )
    rd3tools.status_msg('Manually fixing organisations...')
    del e
    for e in experiment:
        if e['organisation'] == 'Malgorzata  Dec-Cwiek':
            e['organisation'] = 'malgorzata-dec-cwiek'
    rd3tools.status_msg('Processing new data...')
    novelomics_sample = map_rd3_samples(
        data = experiment,
        id_suffix='_novelomics_original',
        subject_suffix="_novelomics_original",
        patch='novelomics_original',
        distinct = True
    )
    novelomics_labinfo = map_rd3_labinfo(
        data = experiment,
        id_suffix = '_novelomics_original',
        sample_id_suffix = '_novelomics_original',
        patch = 'novelomics_original',
        distinct = True
    )
    novelomics_file = map_rd3_files(
        data = experiment,
        sample_id_suffix = "_novelomics_original",
        patch = 'novelomics_original'
    )
    should_import_novelomics = True
    rd3tools.status_msg('Mapped NovelOmics Files: {}'.format(len(novelomics_file)))
    rd3tools.status_msg('Mapped NovelOmics Labinfo: {}'.format(len(novelomics_labinfo)))
    rd3tools.status_msg('Mapped NovelOmics Samples: {}'.format(len(novelomics_sample)))
else:
    rd3tools.status_msg('No experiment metadata available for processing :)')

#//////////////////////////////////////

# ~ 3 ~
# Import Metadata where applicable


if should_import_novelomics_subject:
    rd3tools.status_msg('Importing novel omics subject metadata...')
    rd3.update_table(
        data = novelomics_subject,
        entity = 'rd3_novelomics_subject'
    )
    rd3.update_table(
        data = novelomics_subjectinfo,
        entity = 'rd3_novelomics_subjectinfo'
    )
    update_portal_shipment_tbl(metadata = new_metadata)
    update_portal_shipment_tbl(metadata = old_metadata)
    rd3tools.status_msg('Done!')
else:
    rd3tools.status_msg


# import novel omics data if applicable
if should_import_novelomics_experiment:
    rd3tools.status_msg('Importing novel omics samples metadata...')
    rd3.update_table(
        data = novelomics_sample,
        entity = 'rd3_novelomics_sample'
    )
    rd3tools.status_msg('Importing novel omics labinfo metadata...')
    rd3.update_table(
        data = novelomics_labinfo,
        entity = 'rd3_novelomics_labinfo_wgs'
    )
    rd3tools.status_msg('Importing novel omics file metadata...')
    rd3.update_table(
        data = novelomics_file,
        entity = 'rd3_novelomics_file'
    )
    rd3tools.status_msg('Updating Portal Tables: setting `processed to `True`')
    update_portal_experiment_tbl(data = experiment)

# FIN!
rd3tools.status_msg('Done!! :-)')
# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_mapping_01_main.py
# AUTHOR: David Ruvolo
# CREATED: 2021-04-15
# MODIFIED: 2021-06-23
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

#//////////////////////////////////////////////////////////////////////////////

# init session
rd3tools.status_msg('Processing NovelOmics Data...')
rd3 = rd3tools.molgenis(
    url=config['hosts'][config['run']['env']],
    token=config['tokens'][config['run']['env']]
)

# fetch all reference data
rd3tools.get('Fetching RD3 reference entities')
rd3_organisations = rd3.get('rd3_organisation')
rd3_ERN = rd3.get('rd3_ERN')

# fetch raw novelomics data from the portal 
rd3tools.status_msg('Fetching data from the portal')
experiment = rd3.get(
    entity = 'rd3_portal_novelomics_experiment',
    # q='processed==false',
    batch_size=10000
)

metadata = rd3.get(
    'rd3_portal_novelomics_shipment',
    # q = 'processed==false',
    batch_size = 10000
)


# determine if mapping is necessary
should_map = False
if experiment and metadata:
    rd3tools.status_msg('Data is available to process :-)')
    should_map = True
else:
    rd3tools.status_msg('No new records to process. :-)')


# map data
if should_map:
    rd3tools.status_msg('Processing ERN data...')
    ern_refs = rd3tools.flatten_attr(data=rd3_ERN, attr='identifier')
    metadata = recode_erns(data=metadata, refs=ern_refs, attr='ERN')
    rd3tools.status_msg('Manually fixing organisations...')
    for m in metadata:
        if m['organisation'] == 'Malgorzata  Dec-Cwiek':
            m['organisation'] = 'malgorzata-dec-cwiek'
    rd3tools.status_msg('Processing new data...')
    novelomics = merge_affiliation_data(
        x = experiment,
        y = metadata
    )
    novelomics_subject = map_rd3_subject(
        data = metadata,
        patch = 'novelomics_original'
    )
    novelomics_subjectinfo = map_rd3_subjectinfo(data = novelomics_subject)
    novelomics_sample = map_rd3_samples(
        data = novelomics,
        id_suffix='_novelomics_original',
        subject_suffix="_novelomics_original",
        patch='novelomics_original',
        distinct = True
    )
    novelomics_labinfo = map_rd3_labinfo(
        data = novelomics,
        id_suffix = '_novelomics_original',
        sample_id_suffix = '_novelomics_original',
        patch = 'novelomics_original',
        distinct = True
    )
    novelomics_file = map_rd3_files(
        data = novelomics,
        sample_id_suffix = "_novelomics_original",
        patch = 'novelomics_original'
    )
    should_import_novelomics = True
    rd3tools.status_msg('Mapped NovelOmics Files: {}'.format(len(novelomics_file)))
    rd3tools.status_msg('Mapped NovelOmics Labinfo: {}'.format(len(novelomics_labinfo)))
    rd3tools.status_msg('Mapped NovelOmics Samples: {}'.format(len(novelomics_sample)))
    rd3tools.status_msg('Mapped NovelOmics Subjects: {}'.format(len(novelomics_subject)))


# import novel omics data if applicable
if should_import_novelomics:
    rd3tools.status_msg('Importing novel omics subject metadata...')
    rd3.update_table(
        data = novelomics_subject,
        entity = 'rd3_novelomics_subject'
    )
    rd3.update_table(
        data = novelomics_subjectinfo,
        entity = 'rd3_novelomics_subjectinfo'
    )
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
    update_portal_experiment_tbl(data = novelomics)
    update_portal_shipment_tbl(metadata = metadata)

# FIN!
print('Done!! :-)')

#//////////////////////////////////////////////////////////////////////////////

# @title evaluate patch values
# @description for subjects with existing patch information, extract and
#   collapse new patch information with existing
# @param data a single dictionary containing subject metadata
# @param patch new patch information
# @return a dictionary with the updated patch information
# def proc_subject_patch(data, patch):
#     out = []
#     if len(data['patch']) == 1:
#         out['patch'] = data['patch'][0]['id'] + ',' + patch
#     else:
#         out['patch'] = patch
#     return out

# freeze1_subjects = rd3.get('rd3_freeze1_subject',attributes= api['attribs']['subject'],batch_size=10000)
# freeze2_subjects = rd3.get('rd3_freeze2_subject',attributes= api['attribs']['subject'],batch_size=10000)

# flatten subject IDs
# freeze1_ids = rd3tools.flatten_attr(freeze1_subjects, 'subjectID')
# freeze2_ids = rd3tools.flatten_attr(freeze2_subjects, 'subjectID')


# Triage all records from the staging area. Use subject ID to determine if the
# record belongs in Freeze1 or Freeze2. We don't need to do anything with new cases
# as they don't exist in Freeze1 or 2. It's best to leave them for now until they
# are released in a freeze.
# print('Triaging records...')
# rd3_freeze1 = []
# rd3_freeze2 = []
# rd3_new = []
# for d in experiment:
#     if not d.get('processed'):
#         if d.get('subject_id') in freeze1_ids:
#             rd3_freeze1.append(d)
#         elif d.get('subject_id') in freeze2_ids:
#             rd3_freeze2.append(d)
#         else:
#             rd3_new.append(d)

# print('Freeze 1 records to process:', len(rd3_freeze1))
# print('Freeze 2 records to process:', len(rd3_freeze2))
# print('New records:', len(rd3_new))

# # validate triage
# if sum(map(len, [rd3_freeze1, rd3_freeze2, rd3_new])) == len(experiment):
#     print('Triaged all records')
# else:
#     print('Unable to triage all data')

# get unique IDs for freeze1 and freeze2 patients
# rd3_freeze1_ids = rd3tools.flatten_attr(rd3_freeze1, 'subject_id', distinct=True)
# rd3_freeze2_ids = rd3tools.flatten_attr(rd3_freeze2, 'subject_id', distinct=True)

#//////////////////////////////////////////////////////////////////////////////

# set import flags
# should_import_freeze_1 = False
# should_import_freeze_2 = False

# process freeze1 data
# if len(rd3_freeze1):
#     print('Mapping new freeze1 data...')
#     rd3_freeze1 = merge_affiliation_data(
#         data=rd3_freeze1,
#         metadata=freeze1_subjects
#     )
#     rd3_freeze1_file = map_rd3_files(
#         data=rd3_freeze1,
#         sample_id_suffix="_novelomics_original",
#         patch='novelomics_original'
#     )
#     rd3_freeze1_labinfo = map_rd3_labinfo(
#         data=rd3_freeze1,
#         id_suffix = '_novelomics_original',
#         sample_id_suffix = '_novelomics_original',
#         patch = 'novelomics_original',
#         distinct = True
#     )
#     rd3_freeze1_sample = map_rd3_samples(
#         data=rd3_freeze1,
#         id_suffix='_novelomics_original',
#         subject_suffix="_original",
#         patch='novelomics_original',
#         distinct = True
#     )
#     rd3_freeze1_subject = update_rd3_subject(
#         data = freeze1_subjects,
#         ids = rd3_freeze1_ids,
#         patch = 'novelomics_original'
#     )
#     should_import_freeze_1 = True
#     print('Mapped Freeze1 Files:', len(rd3_freeze1_file))
#     print('Mapped Freeze1 Labinfo:', len(rd3_freeze1_labinfo))
#     print('Mapped Freeze1 Samples:', len(rd3_freeze1_sample))
#     print('Updated Freeze1 Subjects:', len(rd3_freeze1_subject))
# else:
#     print('No new freeze1 records to map')

# # process freeze2 data
# if len(rd3_freeze2):
#     print('Mapping new freeze2 data...')
#     rd3_freeze2 = merge_affiliation_data(
#         data=rd3_freeze2,
#         metadata=freeze2_subjects
#     )
#     rd3_freeze2_file = map_rd3_files(
#         data=rd3_freeze2,
#         sample_id_suffix='_novelomics_original',
#         patch='novelomics_original'
#     )
#     rd3_freeze2_labinfo = map_rd3_labinfo(
#         data=rd3_freeze2,
#         id_suffix = '_novelomics_original',
#         sample_id_suffix = '_novelomics_original',
#         patch = 'novelomics_original',
#         distinct = True
#     )
#     rd3_freeze2_sample = map_rd3_samples(
#         data=rd3_freeze2,
#         id_suffix='_novelomics_original',
#         subject_suffix="_original",
#         patch='novelomics_original',
#         distinct = True
#     )
#     rd3_freeze2_subject = update_rd3_subject(
#         data = freeze2_subjects,
#         ids = rd3_freeze2_ids,
#         patch = 'novelomics_original'
#     )
#     should_import_freeze_2 = True
#     print('Mapped Freeze2 Files:', len(rd3_freeze2_file))
#     print('Mapped Freeze2 Labinfo:', len(rd3_freeze2_labinfo))
#     print('Mapped Freeze2 Samples:', len(rd3_freeze2_sample))
#     print('Updated Freeze2 Subjects:', len(rd3_freeze2_subject))
# else:
#     print('No new freeze2 records to map')
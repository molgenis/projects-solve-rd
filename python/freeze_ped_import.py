#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_ped_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2021-06-29
#' PURPOSE: extract metadata from PED files and import into Molgenis
#' STATUS: in.progress
#' PACKAGES: molgenis.client, ...
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
import os
import re
from datetime import datetime

# read config
config = rd3tools.load_yaml_config("python/_config.yml")


# @title recode_sex
# @description recode PED coding into RD3 terminology
# @param value a string containing PED sex code
# @return a string
def recode_sex(value):
    if value == '1': return 'M'
    elif value == '2': return 'F'
    elif value.lower() == 'other': return "U"
    else: rd3tools.status_msg('ERROR: unable to recode {}'.format(value))

# @title recode_affected_status
# @description recode PED affected status into RD3 terminology
# @param value a string containing a PED affected_status code
# @return a string
def recode_affected_status(value):
    if value in ['-9', '0']: return None
    elif value == '1': return False
    elif value == '2': return True
    else: rd3tools.status_msg('ERROR: unable to recode {}'.format(value))


#//////////////////////////////////////////////////////////////////////////////

# init session
rd3tools.status_msg('Initializing Molgenis Session...')
rd3 = rd3tools.molgenis(
    url=config['hosts'][config['run']['env']],
    token=config['tokens'][config['run']['env']]
)

# load patient metadata for comparision
rd3tools.status_msg('Pulling subject metadata for validation...')
freeze_subject_metadata = rd3.get(
    entity = config['releases'][config['run']['release']]['subject'], # rd3_freeze*_subject
    # q = 'patch=freeze1_patch1',
    attributes='id,subjectID,sex1'
)
subject_ids = rd3tools.flatten_attr(data=freeze_subject_metadata, attr='subjectID')

if freeze_subject_metadata:
    rd3tools.status_msg('Success')
else:
    rd3tools.status_msg('Failed')

# load files metadata to compare md5sums
rd3tools.status_msg('Pulling file metadata for validation')
freeze_files_metadata = rd3.get(
    entity = config['releases'][config['run']['release']]['file'],
    attributes = 'EGA,name,md5',
    q = 'typeFile==ped'
)

if freeze_files_metadata:
    rd3tools.status_msg('Success')
else:
    rd3tools.status_msg('Failed :-(')
    raise SystemExit('Unable to retrieve file metadata. This is mandatory!')

# Create filename column
for freeze_file in freeze_files_metadata:
    freeze_file['filename'] = os.path.basename(freeze_file['name'])
    freeze_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze_file['filename'])[0]

#//////////////////////////////////////

# gather a list of available PED files
rd3tools.status_msg('Gathering list of PED files...')
available_ped_files_raw = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases'][config['run']['release']]['ped']
)

# quick check
if available_ped_files_raw:
    rd3tools.status_msg('Success')
else:
    rd3tools.status_msg('Failed')
    raise SystemExit('Unable to collate available PED files')


# remove non PED files (for now)
available_ped_files = []
for file in available_ped_files_raw:
    if re.search(r'(\.ped|\.ped.cip)$', file.get('file_name')):
        available_ped_files.append(file)
        

# process files
raw_ped_data = []
starttime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
for pedfile in available_ped_files:
    rd3tools.status_msg('Processing file {}'.format(pedfile['file_name']))
    result = rd3tools.find_dict(
        data = freeze_files_metadata,
        attr = 'filename',
        value = pedfile['file_name'] + '.cip'
    )
    should_process = False
    if result:
        rd3tools.status_msg('Evaluating checksums with file metadata')
        md5_result = rd3tools.cluster_run_checksum(path = pedfile['file_path'])
        if result[0]['md5'] == md5_result:
            rd3tools.status_msg('Checksum differs. Data will be processed')
            should_process = True
        else:
            rd3tools.status_msg('Checksum is the same. Moving to next file')
            continue
    else:
        rd3tools.status_msg(
            'File {} does not exist in current freeze'
            .format(pedfile['file_name'])
        )
    if should_process:
        rd3tools.status_msg('Parsing file')
        raw_ped = rd3tools.cluster_read_file(path=pedfile['file_path'])
        data = []
        for line in raw_ped:
            d = line.split()
            if len(d) == 6:
                subject = {
                    'id': d[1],
                    'subjectID': d[1],
                    'fid': d[0],
                    'mid': d[3],
                    'pid': d[2],
                    'sex1': recode_sex(value=d[4]),
                    'clinical_status': recode_affected_status(value=d[5]),
                    'upload': True
                }
                if ('FAM' in subject['id']) or (subject['id'] not in subject_ids):
                    rd3tools.status_msg(
                        'ID {} from family {} does not exist'
                        .format(subject['id'], subject['fid'])
                    )
                    subject['upload'] = False
                if ('FAM' in subject['mid']) or (subject['mid'] == '0') or (subject['mid'] not in subject_ids) :
                        subject['error_mid']=subject['mid']
                        rd3tools.status_msg(
                            'removed mid {} as it starts with `FAM`, is `0`, or it does not exist'
                            .format(subject['mid'])
                        )
                        subject['mid']=None
                if ('FAM' in subject['pid']) or (subject['pid'] == '0') or (subject['pid'] not in subject_ids):
                        subject['error_pid']=subject['pid']
                        rd3tools.status_msg(
                            'removed pid {} as it starts with `FAM`, is `0`, or it does not exist'
                            .format(subject['pid'])
                        )
                        subject['pid']=None
                raw_ped_data.append(subject)
            else:
                rd3tools.status_msg(
                    'Line in {} does not have six columns (has: {})'
                    .format(pedfile['file_name'], len(d))
                )
        should_process = False

# print summary
endtime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
rd3tools.status_msg(
    "Processed PED files in {}"
    .format(
        datetime.strptime(endtime,'%H:%M:%S.%f') - datetime.strptime(starttime, '%H:%M:%S.%f')
    )
)

# file data and prepare for import
rd3tools.status_msg('Removing cases where `upload=False`')
pedigree_data = []
for pf in raw_ped_data:
    if pf['upload']:
        pedigree_data.append(pf)

# validate PED data with phenopackets or other sources
rd3tools.status_msg('Validating Sex codes and updating ID')
patient_sex_codes_validation = []
for d in pedigree_data:
    q = rd3tools.find_dict(
        data = freeze_subject_metadata,
        attr = 'subjectID',
        value = d['subjectID']
    )[0]
    if q:
        d['id'] = q['id']
        if ('sex1' in d) and ('sex1' in q):
            if d['sex1'] == q['sex1']['identifier']:
                rd3tools.status_msg('Sex codes are identical')
            if d['sex1'] != q['sex1']['identifier']:
                rd3tools.status_msg(
                    'Sex codes for {} do not match: PED={}, RD3={}'
                    .format(
                        d['subjectID'],
                        q['sex1']['identifier'],
                        d['sex1']
                    )
                )
                patient_sex_codes_validation.append({
                    'subjectID': d['subjectID'],
                    'rd3_freeze_sex1': q['sex1']['identifier'],
                    'ped_file_sex1': d['sex1']
                })
    else:
        rd3tools.status_msg('Error: no match for {} found'.format(d['subjectID']))

# Warn if cases exist, manually verify each one and correct where applicable
if patient_sex_codes_validation:
    raise SystemError('Inconsistencies detected in sex codes. Manually verify each case')

[print(d) for d in patient_sex_codes_validation]

# del d, q

# format id, mid, pid
for d in pedigree_data:
    subject_q = rd3tools.find_dict(
        data = freeze_subject_metadata,
        attr = 'subjectID',
        value = d['subjectID']
    )[0]
    if subject_q:
        d['id'] = subject_q['id']
    else:
        rd3tools.status_msg(
            'Error: no match for {} found in RD3'
            .format(d['subjectID'])
        )
    if d['mid']:
        mid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['mid']
        )[0]
        if mid_q:
            d['mid'] = mid_q['id']
        else:
            rd3tools.status_msg(
                'given maternal ID {} does not exist in RD3'
                .format(d['mid'])
            )
    if d['pid']:
        pid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['pid']
        )[0]
        if pid_q:
            d['pid'] = pid_q['id']
        else:
            rd3tools.status_msg(
                'given paternal ID {} does not exist in RD3'
                .format(d['pid'])
            )
    

# upload data
rd3tools.status_msg('Prepping data for import...')
upload_fid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'fid'])
upload_mid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'mid'])
upload_pid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'pid']) 
upload_clinical = rd3tools.select_keys(data = pedigree_data, keys = ['id','clinical_status'])

rd3tools.status_msg('Importing data')
rd3.batch_update_one_attr(entity='rd3_freeze2_subject', attr='fid', values=upload_fid)
rd3.batch_update_one_attr(entity='rd3_freeze2_subject', attr='mid', values=upload_mid)
rd3.batch_update_one_attr(entity='rd3_freeze2_subject', attr='pid', values=upload_pid)
rd3.batch_update_one_attr(entity='rd3_freeze2_subject', attr='clinical_status', values=upload_clinical)
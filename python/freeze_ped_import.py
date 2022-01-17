#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_ped_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2022-01-17
#' PURPOSE: extract metadata from PED files and import into Molgenis
#' STATUS: stable
#' PACKAGES: *see imports below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
from dotenv import load_dotenv
from os import environ, path
from datetime import datetime
import re

# set vars
load_dotenv()
currentReleaseType='patch' # or 'release'
currentFreeze = 'freeze1' # 'freeze2'
currentPatch = 'patch3' # 'patch1'
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

# build entity IDs and paths based on current freeze and patch
paths = rd3tools.build_rd3_paths(
    freeze = currentFreeze,
    patch = currentPatch,
    baseFilePath = environ['CLUSTER_BASE']
)

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~ 
# Start Molgenis Session and Pull Required Data
#
# For processing the PED files, we will need the following attributes from the
# RD3 `rd3_freeze[x]_subject` where `[x]` is the freeze that the new PED files
# are tied to (e.g., `rd3_freeze2_subject`).
#
#   - `id`: the molgenis row ID; a concatenation of subject ID and release
#   - `subjectID`: RD3 P number
#   - `sex`: patient's sex
#
# It isn't necessary to run extensive checks that compare PED file data with
# the values that are in RD3 as PED files should be considered the most
# up to date.

# init session
rd3 = rd3tools.molgenis(url = host, token = token)

# pull subject metadata for the current freeze
freeze_subject_metadata = rd3.get(
    entity = paths['rd3_subjects'],
    # q = 'patch=freeze1_patch1',
    attributes='id,subjectID,sex1',
    batch_size = 100000
)

# flatten subjectIDs for faster comparison later on
subject_ids = rd3tools.flatten_attr(data=freeze_subject_metadata, attr='subjectID')

#
# In addition to subject metadata, it is import to pull file metadata to identify
# which have changed and should be processed. We will pull the following
# attributes:
#
#   - `EGA`: the EGA file ID
#   - `name`: the full name of the file
#   - `md5`: checksum
#

# pull file metadata
freeze_files_metadata = rd3.get(
    entity = paths['rd3_files'],
    attributes = 'EGA,name,md5',
    q = 'typeFile==ped'
)

# For each record retrieved, add a column `filename` that receives the basename
# of the full file name.
for freeze_file in freeze_files_metadata:
    freeze_file['filename'] = path.basename(freeze_file['name'])
    freeze_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze_file['filename'])[0]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process PED files and extract content
#
# Compile a lost of all available PED files on the cluster and process each
# file individually. The processing step parses and restructures the contents
# into RD3 terminology. PED files are essentially tab separated text files
# that have the following columns. Each file is a family of a RD3 subject where
# each line is an individual. Files should have at least one row (the subject).
# Other lines contain data on family members if data was supplied by the data
# submitter.
#
# PED files contain six columns. 
#
#   0. Family ID
#   1. Individual ID
#   2. Paternal ID
#   3. Maternal ID
#   4. Sex: options are "1" (M), "2" (F), or "OTHER" (U)
#   5. Affected Status: "-9" (None), "0" (None), "1" (False), or "2" (True)
#
# See https://zzz.bwh.harvard.edu/plink/data.shtml for more information.
#
# By default all rows are given an "upload" status. If any of the validation
# steps fail, the upload status is set to FALSE. See the script `rd3tools.py`
# for more information on how values are validated.

# Create a list of all PED files that are stored at a specific path on the
# cluster. Make sure all non-PED files are removed.
available_ped_files_raw = rd3tools.cluster_list_files(path = paths['cluster_ped'])
available_ped_files = []
for file in available_ped_files_raw:
    if re.search(r'(\.ped|\.ped.cip)$', file.get('file_name')):
        available_ped_files.append(file)
        

# ~ 2a ~
# For each file, extract contents, validate data and transform into RD3
# terminology.
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
        if result[0]['md5'] != md5_result:
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
        contents = rd3tools.cluster_read_file(path = pedfile['file_path'])
        data = rd3tools.ped_extract_contents(
            contents = contents,
            ids = subject_ids,
            fileaname = file['file_name']
        )
        raw_ped_data.append(data)
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
        
#//////////////////////////////////////

# ~ 2b ~
# Validate new PED data against RD3 data
#
# For all records where "upload" is TRUE, validate relevant attributes against
# values that exist in RD3. At the moment, we are validating sex codes. It is
# unlikely that these values will change since the initial release, but it is
# good to confirm the values in case there was an issue somewhere in the data
# processing, file generation, or somewhere else. 
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
                    .format(d['subjectID'], q['sex1']['identifier'], d['sex1'])
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
del d, q

#//////////////////////////////////////

# ~ 2c ~
# Validate and Merge RD3 data with new PED data
#
# Before the data can be imported into RD3, it is import to confirm that all IDs
# exist in RD3. Since ID columns are referenced by other columns, all unknown
# IDs will throw an error. The following step will examine subject ID, 
# maternal ID, and paternal ID. IDs that are listed in the PED files will be
# used to search the `rd3_freeze[x]_subject` table. If the ID exists, the ID
# will be added to the new record.
#
# At this point, it isn't necessary to investigate unknown IDs.
#

for d in pedigree_data:
    subject_q = rd3tools.find_dict(
        data = freeze_subject_metadata,
        attr = 'subjectID',
        value = d['subjectID']
    )[0]
    # Merge corresponding table key
    if subject_q:
        d['id'] = subject_q['id']
    else:
        rd3tools.status_msg('Error: no match for {} found in RD3'.format(d['subjectID']))
    # Does the maternal ID exist?
    if d['mid']:
        mid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['mid']
        )[0]
        if mid_q:
            d['mid'] = mid_q['id']
        else:
            rd3tools.status_msg('given maternal ID {} does not exist in RD3'.format(d['mid']))
    # Does the paternal ID exist?
    if d['pid']:
        pid_q = rd3tools.find_dict(
            data = freeze_subject_metadata,
            attr = 'subjectID',
            value = d['pid']
        )[0]
        if pid_q:
            d['pid'] = pid_q['id']
        else:
            rd3tools.status_msg('given paternal ID {} does not exist in RD3'.format(d['pid']))

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data
#
# Importing data into RD3 is rather straightforward. All data is imported into
# subjects table of the current freeze.
#

# select keys
upload_fid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'fid'])
upload_mid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'mid'])
upload_pid = rd3tools.select_keys(data = pedigree_data, keys = ['id', 'pid']) 
upload_clinical = rd3tools.select_keys(data = pedigree_data, keys = ['id','clinical_status'])

# import into the subjects table of the current freeze
rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='fid', values=upload_fid)
rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='mid', values=upload_mid)
rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='pid', values=upload_pid)
rd3.batch_update_one_attr(entity=paths['rd3_subjects'], attr='clinical_status', values=upload_clinical)
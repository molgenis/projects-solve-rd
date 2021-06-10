#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_ped_import.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-02
#' MODIFIED: 2021-06-02
#' PURPOSE: extract metadata from PED files and import into Molgenis
#' STATUS: in.progress
#' PACKAGES: molgenis.client, ...
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import molgenis.client as molgenis
import json
import yaml
import subprocess
import re
from datetime import datetime

# read config
with open('python/_config.yml', 'r') as f:
    config = yaml.safe_load(f)

# @title timestamp
def timestamp():
    return datetime.utcnow().strftime('%Y-%m-%d %H:%H:%S.%f')[:-3]

# @title Molgenis Extra
# @describe Add script-specific methods to molgenis.Session class
# @param molgenis.Session required param
# @return ...
class molgenis_extra(molgenis.Session):
    def update_table(self, data, entity):
        for d in range(0, len(data), 1000):
            response = self._session.post(
                url=self._api_url + 'v2/' + entity,
                headers=self._get_token_header_with_content_type(),
                data=json.dumps({'entities': data[d:d+1000]})
            )
            if response.status_code == 201:
                print("Imported batch " + str(d) +
                      " successfully (" + str(response.status_code) + ")")
            else:
                print("Failed to import batch " + str(d) +
                      " (" + str(response.status_code) + ")")


# @title list_ped_files
# @description from a given path, create a list of available files
# @param path location of the files
# @return a list of dictionaries containing file metadata
def list_ped_files(path):
    available_files = subprocess.Popen(
        ['ssh', 'corridor+fender', 'ls', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr= subprocess.PIPE,
        universal_newlines=True
    )
    available_files.wait()
    data = []
    for f in available_files.stdout:
        data.append({
            'file_name': f.strip(),
            'file_path': config['paths']['cluster']['ped'] + f.strip()
        })
    available_files.kill()
    print('Found', len(data), 'files')
    return data

# @title read_ped_file
# @description read ped file from a given path
# @param path location of the PED file to read
# @return a list of lines
def read_ped_file(path):
    file = subprocess.Popen(
        ['ssh', 'corridor+fender', 'cat', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    file.wait()
    data= []
    for line in file.stdout:
        data.append(line.strip())
    file.kill()
    return data


# @title run_ped_checksum(path):
# @description run the md5 command on a file and return the value
# @param path location of the file run the checksum
# @return a string containing an md5 value
def run_ped_checksum(path):
    proc = subprocess.Popen(
        ['ssh', 'corridor+fender', 'md5sum', path],
        stdout = subprocess.PIPE,
        stdin = subprocess.PIPE,
        stderr = subprocess.PIPE,
        universal_newlines=True
    )
    proc.wait()
    value = proc.stdout.read().split()[0]
    proc.kill()
    return value

# @title recode_sex
# @description recode PED coding into RD3 terminology
# @param value a string containing PED sex code
# @return a string
def recode_sex(value):
    if value == '1': return 'M'
    elif value == '2': return 'F'
    elif value.lower() == 'other': return "U"
    else: print('ERROR: unable to recode {}'.format(value))

# @title recode_affected_status
# @description recode PED affected status into RD3 terminology
# @param value a string containing a PED affected_status code
# @return a string
def recode_affected_status(value):
    if value in ['-9', '0']: return 'N/A'
    elif value == '1': return 'No'
    elif value == '2': return 'Yes'
    else: print('ERROR: unable to recode {}'.format(value))

# @title flatten attribute
# @description pull values from a specific attribute
# @param data list of dict
# @param name of attribute to flatten
# @param distinct if TRUE, return unique cases only
# @return a list of values
def flatten_attr(data, attr, distinct=False):
    out = []
    for d in data:
        tmp_attr = d.get(attr)
        out.append(tmp_attr)
    if distinct:
        return list(set(out))
    else:
        return out

# @title filter list of dictionaries
# @param data object to search
# @param attr variable find match
# @param value value to filter for
# @return a list of a dictionary
def find_dict(data, attr, value):
    return list(filter(lambda d: d[attr] in value, data))


#//////////////////////////////////////////////////////////////////////////////

# init session
print('[{}] Initializing Molgenis Session'.format(timestamp()))
rd3 = molgenis_extra(url=config['host'][config['env']], token=config['tokens'][config['env']])

# load patient metadata for comparision
print('[{}] Pulling subject metadata for validation: '.format(timestamp()), end="", flush=True)
freeze2_subject_metadata = rd3.get('rd3_freeze2_subject', attributes='id,subjectID')
subject_ids = flatten_attr(data=freeze2_subject_metadata, attr='subjectID')

if freeze2_subject_metadata:
    print('Success')
else:
    print('Failed')

# load files metadata to compare md5sums
print('[{}] Pulling file metadata for validation:'.format(timestamp()), end="", flush=True)
freeze2_files_metadata = rd3.get('rd3_freeze2_file',attributes='EGA,name,md5', q='typeFile==ped')

if freeze2_files_metadata:
    print('Sucess')
else:
    print('Failed')

for freeze2_file in freeze2_files_metadata:
    freeze2_file['filename'] = os.path.basename(freeze2_file['name'])
    freeze2_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze2_file['filename'])[0]


#//////////////////////////////////////


# gather a list of available PED files
print('[{}] Gathering list of PED files:'.format(timestamp()), end="", flush=True)
available_ped_files = list_ped_files(path=config['paths']['cluster']['ped'])

if available_ped_files:
    print('Success')
else:
    print('Failed')
    raise SystemExit('Unable to collate available PED files')


# process files
raw_ped_data = []
starttime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
for pedfile in available_ped_files[:5]:
    print('[{}] Processing file {}'.format(timestamp(), pedfile['file_name']))
    result = find_dict(data=freeze2_files_metadata, attr='filename', value=pedfile['file_name'])
    should_process = False
    if result:
        print('[{}] Evaluating checksums with file metadata'.format(timestamp()))
        md5_result = run_ped_checksum(path = pedfile['file_path'])
        if result[0]['md5'] == md5_result:
            print('[{}] Checksum differs. Data will be processed'.format(timestamp()))
            should_process = True
        else:
            print('[{}] Checksum is the same. Moving to next file'.format(timestamp()))
            continue
    else:
        print(
            '[{}] File {} does not exist in current freeze'
            .format(timestamp(), pedfile['file_name'])
        )
    if should_process:
        print('[{}] Parsing file'.format(timestamp()))
        raw_ped = read_ped_file(path=pedfile['file_path'])
        data = []
        for line in raw_ped:
            d = line.split()
            if len(d) == 6:
                subject = {
                    'id': d[1],
                    'subjectID': d[1],
                    'fid': d[0],
                    'mid': d[2],
                    'pid': d[3],
                    'sex1': recode_sex(value=d[4]),
                    'clinical_status': recode_affected_status(value=d[5]),
                    'upload': True
                }
                if ('FAM' in subject['id']) or (subject['id'] not in subject_ids):
                    print(
                        '[{}] ID {} from family {} does not exist'
                        .format(timestamp(), subject['id'], subject['fid'])
                    )
                    subject['upload'] = False
                if ('FAM' in subject['mid']) or (subject['mid'] == '0') or (subject['mid'] not in subject_ids) :
                        subject['error_mid']=subject['mid']
                        print(
                            '[{}] removed mid {} as it starts with `FAM`, is `0`, or it does not exist'
                            .format(timestamp(), subject['mid'])
                        )
                        subject['mid']=None
                if ('FAM' in subject['pid']) or (subject['pid'] == '0') or (subject['pid'] not in subject_ids):
                        subject['error_pid']=subject['pid']
                        print(
                            '[{}] removed pid {} as it starts with `FAM`, is `0`, or it does not exist'
                            .format(timestamp(), subject['pid'])
                        )
                        subject['pid']=None
                raw_ped_data.append(subject)
            else:
                print(
                    '[{}] line in {} does not have six columns (has: {})'
                    .format(timestamp(), pedfile['file_name'], len(d))
                )
        should_process = False

# print summary
endtime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
print(
    "[{}] Processed PED files in {}"
    .format(
        timestamp(),
        datetime.strptime(endtime,'%H:%M:%S.%f') - datetime.strptime(starttime, '%H:%M:%S.%f')
    )
)

# file data and prepare for import
pedigree_data = []
for pf in raw_ped_data:
    if pf['upload']:
        pedigree_data.append(pf)
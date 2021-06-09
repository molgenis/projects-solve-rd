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
from python.freeze_phenopackets_import import read_phenopacket
from sys import stderr, stdin, stdout
import molgenis.client as molgenis
import json
import yaml
import subprocess
import re

# read config
with open('python/_config.yml', 'r') as f:
    config = yaml.safe_load(f)

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
# @param cmd connection command to execute
# @param path location of the files
# @return a list of dictionaries containing file metadata
def list_ped_files(cmd, path):
    available_files = subprocess.Popen(
        ['ssh', cmd, 'ls', path],
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
        ['ssh', config['cmd'], 'cat', path],
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

#//////////////////////////////////////

# connect
rd3 = molgenis_extra(url=config['host'][config['env']], token=config['token'][config['env']])

# mappings:
# fid: familyID
# subjectId: subjectID
# mid: maternal ID
# pid: paternal ID
# sex: 1=male, 2=female, other=unknown)
# clinical_status: affected status where -9=missing, 0=missing, 1=unaffected, 2=affected
ped_files = list_ped_files(cmd = config['cmd'], path=config['paths']['cluster']['ped'])

test = read_ped_file(path=ped_files[0]['file_path'])

subject_ids = rd3.get()

data = []
for line in test:
    d = line.split(' ')
    if len(d) == 6:
        subject = {
            'id': d[1],
            'subjectID': d[1],
            'fid': d[0],
            'mid': d[2],
            'pid': d[3],
            'sex1': recode_sex(value=d[4]),
            'clinical_status': recode_affected_status(value=d[5])
        }
        if ('FAM' in subject['id']) or (subject['id'] not in subject_ids):
            subject['upload'] = 'N'
            print(
                'ERROR: ID {} from family {} does not exist'.format(
                    subject['id'], subject['fid']
                )
            )
        if ('FAM' in subject['mid']) or (subject['mid'] == '0') or (subject['mid'] not in subject_ids) :
                subject['error_mid']=subject['mid']
                print(
                    'ERROR: Mother ID {} in family {} starts with FAM is 0 or is not a valid subject ID, mother ID removed'.format(subject['mid'], subject['fid'])
                )
                subject['mid']=None
        if ('FAM' in subject['pid']) or (subject['pid'] == '0') or (subject['pid'] not in subject_ids):
                subject['error_pid']=subject['pid']
                print('ERROR Father ID {} in family folder {} starts with FAM is 0 or is not a valid subject ID, father ID removed'.format(subject['pid'], subject['fid']))
                subject['pid']=None
    else:
        print(
            'ERROR: PED file {} does not contain six columns'.format(
                ped_files[0]['file_name']
            )
        )

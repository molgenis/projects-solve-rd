#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze2_file_metadata.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-28
#' MODIFIED: 2021-05-31
#' PURPOSE: pushing file metadata into RD3
#' STATUS: in.progress
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import re
import json
import molgenis.client as molgenis
from datetime import datetime

# apply methods to class Session
class rd3_session(molgenis.Session):
    def update_table(self, data, entity):
        for d in range(0, len(data), 1000):
            response = self._session.post(
                url = self._url + 'v2/' + entity,
                headers = self._get_token_header_with_content_type(),
                data = json.dumps({'entities': data[d:d+1000]})
            )
            if response.status_code == 201:
                print(
                    'Imported batch ' + str(d) +
                    'successfully (' + str(response.status_code) + ')'
                )
            else:
                print(
                    'Failed to import batch' + str(d) +
                    '(' + str(response.status_code) + ')'
                )

# @title Read File
# @description Read ASCII file and transform into a list of dictionaries
# @param file path to file location
# @return a list of dictionaries containing file metatdata
def read_file(file):
    rawdata = []
    with open(file, 'r', encoding='utf-8') as file:
        for index, line in enumerate(file):
            if (not index in range(7)) and (not line == '') and (not re.search(r'^[-]{1,}', line)):
                elements = re.split(r'\s{1,}', line)
                tmp = {
                    'file_id': None,
                    'status': None,
                    'bytes': None, 
                    'check_sum': None,
                    'file_name': None
                }
                for el in elements:
                    if re.search(r'^(EGAF[0-9]+)$', el):
                        tmp['file_id'] = el
                    if (len(el) == 1) and (re.search(r'^([0-9]{1})$', el)):
                        tmp['status'] = el
                    if re.search(r'^([0-9]{2,})$', el):
                        tmp['bytes'] = el
                    if re.search(r'^([0-9a-z]{32})$', el):
                        tmp['check_sum'] = el
                    if re.search(r'(/SolveRD/)', el):
                        tmp['file_name'] = el
                rawdata.append(tmp)
    return rawdata

# @title Extract Filetype from string
# @description
#   extract filetype from string (needs checks for: bed, cram, fastq)
# @param filename string containing RD3 filepath
# @return a dict containing an filetype and ID
def extract_filetype(filename):
    base = os.path.basename(filename)
    value = None
    if re.search(r'(.g.vcf)', base):
        value = 'vcf'
    if re.search(r'(.bai.)', base):
        value = 'bai'
    if re.search(r'(.bam.)', base):
        value = 'bam'
    if re.search(r'(.ped.)', base):
        value = 'ped'
    if re.search(r'(.json)', base):
        value = 'json'
    return value

# @title Extract ID from filepath
# @description From filepath, extract ID(s)
# @param filename string containing a filepath
# @return a string containing the ID
def extract_id(filename):
    value = None
    result = re.search(r'(E[0-9]{6})', filename)
    if result:
        value = result.group()
    return value

# @title Get missing value count by key
# @description get number of missing values by key
# @param data list of dictionaries to evaluate
# @return dictionary of counts by key
def missing_count(data):
    keys = {k: 0 for k in data[0].keys()}
    for d in enumerate(data):
        for i in d:
            if (type(i) == None) or (i == ''):
                keys[i] += 1
    return keys


#//////////////////////////////////////

# set client config
env = 'acc'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc' : 'https://solve-rd-acc.gcc.rug.nl/api/'
    },
    'token': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
}

# init session
rd3 = rd3_session(url=api['host'][env], token=api['token'])

# load, process, and map metadata
rawdata = read_file(file = '')
mapped_data = []
maxreps = len(rawdata)
for i, d in enumerate(rawdata):
    mapped_data.append({
        'EGA': d['file_id'],
        'name': d['file_name'],
        'md5': d['check_sum'],
        'typeFile': extract_filetype(d['file_name']),
        'samples': extract_id(d['file_name']),
        'dateCreated': datetime.today().strftime('%Y-%m-%d'),
        'patch': 'freeze2_original'
    })

# evaluate missing information and upload
counts = missing_count(mapped_data)
if sum(counts) == 0:
    rd3_session.update_table(data = mapped_data, entity='rd3_freeze2_file')
else:
    SystemError('Error: missing values detected\n' + counts)

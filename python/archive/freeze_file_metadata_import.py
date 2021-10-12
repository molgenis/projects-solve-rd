# '////////////////////////////////////////////////////////////////////////////
# ' FILE: freeze_file_metadata_import.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2021-05-28
# ' MODIFIED: 2021-06-01
# ' PURPOSE: pushing file metadata into RD3
# ' STATUS: complete
# ' PACKAGES: molgenis.client
# ' COMMENTS: NA
# '////////////////////////////////////////////////////////////////////////////

import os
import re
import json
import molgenis.client as molgenis
from datetime import datetime

# apply methods to class Session
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
                if not (all(v is None for v in tmp.values())):
                    rawdata.append(tmp)
    return rawdata



# @title Extract Filetype from string
# @description
#   extract filetype from string (needs checks for: bed, cram, fastq)
# @param filename string containing RD3 filepath
# @return a dict containing an filetype and ID
def extract_filetype(filename):
    base = os.path.basename(filename)
    value = ''
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
    for d in data:
        for i in d:
            if d[i] == None:
                keys[i] += 1
    return keys


# @title Extract Nested Attribute
# @description extract attribute from nested dictionary
# @param data input dataset a list of dictionaries
# @param attr value to extract
# @return a string of values
def extract_nested_attr(data, attr):
    value = None
    if len(data) == 1:
        value = data[0].get(attr)
    if len(data) > 1:
        joined_att = []
        for d in data:
            joined_att.append(d.get(attr))
        value = ','.join(map(str, joined_att))
    return value

# @title Flatten Experiment
# @description Flatten experiment data by extracting elements of interest
# @param data data from rd3_freeze*_labinfo
# @return list of dictionarires
def flatten_labinfo(data):
    out = []
    for d in data:
        tmp = {}
        tmp['id'] = d.get('id')
        tmp['experimentID'] = d.get('experimentID')
        tmp['sample_id'] = extract_nested_attr(d.get('sample'), 'id')
        tmp['sampleID'] = extract_nested_attr(d.get('sample'), 'sampleID')
        out.append(tmp)
    return out

# @title Map Filemetadata
# @description map file metadata into RD3 terminology and merge experimentID
# @param file_data output of `read_file` (list object)
# @param lab_data list of dictionaries from `rd3.get('rd3_freeze*_labinfo')`
# @return list of dictionaries ready for import 
def map_file_metadata(file_data,lab_data):
    out = []
    for d in file_data:
        tmp = {
            'EGA': d.get('file_id'),
            'name': d.get('file_name'),
            'md5': d.get('check_sum'),
            'typeFile': extract_filetype(d.get('file_name')),
            'dateCreated': datetime.today().strftime('%Y-%m-%d'),
            'patch': 'freeze2_original'
        }
        tmp_expr_id = extract_id(d.get('file_name'))
        if tmp_expr_id is not None:
            tmp['experimentID'] = str(tmp_expr_id)
            q = list(filter(lambda el: el['experimentID'] in tmp_expr_id, lab_data))
            if len(q):
                tmp['samples'] = q[0].get('sample_id')
        out.append(tmp)
    return out

# //////////////////////////////////////

# set client config
env = 'acc'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc': 'https://solve-rd-acc.gcc.rug.nl/api/'
    },
    'token': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
}

# init session
rd3 = molgenis_extra(url=api['host'][env], token=api['token'])

# pull experiment info
rd3_freeze2_labinfo_raw = rd3.get(
    entity='rd3_freeze2_labinfo',
    attributes='id,experimentID,sample',
    batch_size=10000
)

rd3_freeze2_labinfo = flatten_labinfo(data = rd3_freeze2_labinfo_raw)

# load raw metadata file
print('Loading and processing data file...')
metadata_raw = read_file(file='')

print('Mapping metadata into RD3 terminology...')
metadata = map_file_metadata(file_data = metadata_raw, lab_data=rd3_freeze2_labinfo)

# upload
print('Uploading metadata...')
rd3.update_table(data = metadata, entity='rd3_freeze2_file')
#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze2_file_metadata.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-28
#' MODIFIED: 2021-05-28
#' PURPOSE: pushing file metadata into RD3
#' STATUS: in.progress
#' PACKAGES: molgenis.client
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import re
import molgenis.client as molgenis
from datetime import datetime

# set client config
env = 'dev'
api = {
    'host': {
        'prod': 'https://solve-rd.gcc.rug.nl/api/',
        'acc' : 'https://solve-rd-acc.gcc.rug.nl/api/',
        'dev' : 'https://solve-rd-acc.gcc.rug.nl/api/',
    },
    'token': {
        'prod': '${molgenisToken}',
        'acc': '${molgenisToken}',
        'dev': os.getenv('molgenisToken') if os.getenv('molgenisToken') is not None else None
    }
}

# init session
rd3 = molgenis.Session(url=api['host'][env], token=api['token'][env])

# @title Read File
# @description Read ASCII file and transform into a list of dictionaries
# @param file path to file location
# @return a list of dictionaries containing file metatdata
def read_file(file):
    rawdata = []
    with open(file, 'r', encoding='utf-8') as file:
        for index, line in enumerate(file):
            if (not index in range(7)) and (not line == '') and (not re.search(r'^[-]{1,}', line)):
                l = re.split(r'\s{1,}', line)
                if len(l) == 5:
                    tmp = {}
                    elements = l
                    for el in elements:
                        if re.search(r'^(EGAF)[0-9]+', el):
                            tmp['file_id'] = el
                            next(el)
                        if re.search(r'[0-9]{1}', el):
                            tmp['status'] = el
                            next(el)
                        if re.search(r'[0-9]+', el):
                            tmp['bytes'] = el
                            next(el)
                        if re.search(r'[0-9a-zA-Z]{32}', el):
                            tmp['check_sum'] = el
                            next(el)
                        if re.search(r'prod/hl-ega-box-1219-CNAG/SolveRD', el):
                            tmp['file_name'] = el
                            next(el)
                    rawdata.append(tmp)
                else:
                    print('Error at index:', index, '. Mismatched dimensions')
    return rawdata

# @title Map metadata to RD3
# @description map rawdata into Rd3 terminology
# @param data output of `read_file`
# @return list of dictionaries
def map_metadata_to_rd3(data):
    out = []
    for d in data:
        el = {}
        el['EGA'] = d.get('file_id')
        el['name'] = d.get('file_name')
        el['md5'] = d.get('check_sum')
        # el['typeFile'] = some_function(d['file_name'])
        # el['samples']  = some_function(d['file_name'])
        el['dateCreated'] = datetime.today().strftime('%Y-%m-%d')
        el['patch'] = 'freeze2_original'
        out.append({ el })
    return out


# load, process, and map metadata
rawdata = read_file(file = '...')
mapped_dat = map_metadata_to_rd3(data = rawdata)

# push to Molgenis?
# isHeader = True if re.search(r'(File\sID)\s{1,}(Status)\s{1,}(Bytes)\s{1,}(Check\ssum)\s{1,}(File\sname)', line) else False
#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_update_subjectinfo.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-08
#' MODIFIED: 2021-06-08
#' PURPOSE: update subjectinfo table
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import os
import molgenis.client as molgenis


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

# set client config
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
    },
    'attribs': {
        'subject': 'id,subjectID,patch'
    }
}

# init
env = 'acc'
rd3 = molgenis.Session(url=api['host'][env], token=api['token'][env])


# pull data
freeze1_subjects = rd3.get('rd3_freeze1_subject', attributes=api['attribs']['subject'])
freeze1_subjectinfo = rd3.get('rd3_freeze1_subjectinfo', attributes=api['attribs']['subject'])

freeze2_subjects = rd3.get('rd3_freeze2_subject', attributes=api['attribs']['subject'])
freeze2_subjectinfo = rd3.get('rd3_freeze2_subjectinfo', attributes=api['attribs']['subject'])

# function for finding and flattening records to update
def find_missing_subjectinfo_records(subjects, subjectinfo):
    subjectinfo_ids = flatten_attr(subjectinfo, 'id')
    out = []
    for subject in subjects:
        if not (subject['id'] in subjectinfo_ids):
            if len(subject['patch']) >= 1:
                tmp_patches = flatten_attr(subject['patch'], 'id')
                subject['subjectID'] = subject['id']
                subject['patch'] = ','.join(map(str, tmp_patches))
            out.append(subject)
    return out

# locate which cases aren't in subjectinfo
missing_freeze1_subjectinfo = find_missing_subjectinfo_records(freeze1_subjects, freeze1_subjectinfo)
missing_freeze2_subjectinfo = find_missing_subjectinfo_records(freeze2_subjects, freeze2_subjectinfo)


# push into RD3
rd3.add_all('rd3_freeze1_subjectinfo',missing_freeze1_subjectinfo)
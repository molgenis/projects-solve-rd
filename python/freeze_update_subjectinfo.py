#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_update_subjectinfo.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-08
#' MODIFIED: 2021-06-09
#' PURPOSE: update subjectinfo table
#' STATUS: refresh subjectinfo table
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

# @title find missing subject records
# @description find records from `rd3_freeze*_subject` that aren't in `rd3_freeze*_subjectinfo`
# @param subjects output from `rd3_freeze*_subject` (a list of dictionaries)
# @param subjectinfo output from `rd3_freeze*_subjectinfo` (a list of dictionaries)
# @return a list of dictionaries containing subjects to add to `rd3_freeze*_subjectinfo`
def find_missing_records(subjects, subjectinfo):
    subjectinfo_ids = flatten_attr(subjectinfo, 'id')
    out = []
    for subject in subjects:
        if not (subject['id'] in subjectinfo_ids):
            tmp = subject
            if len(subject['patch']) >= 1:
                tmp_patches = flatten_attr(tmp['patch'], 'id')
                tmp['subjectID'] = subject['id']
                tmp['patch'] = ','.join(map(str, tmp_patches))
            out.append(tmp)
    return out

#//////////////////////////////////////////////////////////////////////////////

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


# process freeze1 data
freeze1_subjects = rd3.get('rd3_freeze1_subject', attributes=api['attribs']['subject'])
freeze1_subjectinfo = rd3.get('rd3_freeze1_subjectinfo', attributes=api['attribs']['subject'])

missing_freeze1_subjectinfo = find_missing_records(freeze1_subjects, freeze1_subjectinfo)
rd3.add_all('rd3_freeze1_subjectinfo',missing_freeze1_subjectinfo)


# process freeze2 data
freeze2_subjects = rd3.get('rd3_freeze2_subject', attributes=api['attribs']['subject'])
freeze2_subjectinfo = rd3.get('rd3_freeze2_subjectinfo', attributes=api['attribs']['subject'])

missing_freeze2_subjectinfo = find_missing_records(freeze2_subjects, freeze2_subjectinfo)
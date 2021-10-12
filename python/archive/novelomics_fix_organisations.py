#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_fixes.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-07-05
#' MODIFIED: 2021-07-05
#' PURPOSE: script for one time only fixes to metadata
#' STATUS: on.going
#' PACKAGES: rd3tools
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


import python.rd3tools as rd3tools
config = rd3tools.load_yaml_config('python/_config.yml')

# init session
rd3 = rd3tools.molgenis(
    url = config['hosts']['prod'],
    token = config['tokens']['prod']
)

#//////////////////////////////////////

# ~ 1 ~
# Fix organisations grouping

# get data to change
novelomics_subjects = rd3.get(
    entity = 'rd3_novelomics_subject',
    q = 'organisation=="nimhans-atchayaram"',
    attributes = 'id,subjectID,organisation',
    batch_size = 10000
)

# check
len(novelomics_subjects)


# fix mapping
upload = []
for subject in novelomics_subjects:
    upload.append({
        'id': subject.get('id'),
        'organisation': 'cheo-lochmuller'
    })

len(upload)

# import
rd3.batch_update_one_attr(
    entity = 'rd3_novelomics_subject',
    attr = 'organisation',
    values = upload
)
#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_update_cov.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-07-20
#' MODIFIED: 2021-07-20
#' PURPOSE: update MeanCov and C20 attributes in Labinfo
#' STATUS: working
#' PACKAGES: *see below*
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
import pandas as pd
config = rd3tools.load_yaml_config(path = 'python/_config.yml')

# load raw data file
rawdata = pd.read_csv(
    'data/2021-02-01.Freeze2candidates.sample.coverage.metrics.3303.tsv',
    sep = '\t',
    usecols = [0,1,2,3,4,5,6,7]
).to_dict('records')

# reduce dataset and map to RD3 terminology
covdata = rd3tools.select_keys(data = rawdata, keys=['#Sample', 'Mean_cov', 'C20'])
for d in covdata:
    d['experimentID'] = d.pop('#Sample')
    d['mean_cov'] = d.pop('Mean_cov')
    d['c20'] = d.pop('C20')

# pull labinfo data
rd3 = rd3tools.molgenis(config['hosts']['prod'], token = config['tokens']['prod'])

labinfo = rd3.get('rd3_freeze2_labinfo', attributes = 'id,experimentID', batch_size = 10000)
len(labinfo)

# merge data where applicable
for l in labinfo:
    result = rd3tools.find_dict(data = covdata, attr = 'experimentID', value = l['experimentID'])
    if result:
        l['upload'] = 'yes'
        l['mean_cov'] = result[0]['mean_cov']
        l['c20'] = result[0]['c20']
    else:
        l['upload'] = 'no'

# upload entities where upload == 'yes'
labinfo_upload = rd3tools.find_dict(data = labinfo, attr = 'upload', value = 'yes')

print('Uploading {} out of {}'.format(len(labinfo_upload), len(labinfo)))

# create separate objects for uploading in batches
labinfo_meancov = rd3tools.select_keys(data = labinfo_upload, keys = ['id', 'mean_cov'])
labinfo_c20 = rd3tools.select_keys(data = labinfo_upload, keys = ['id', 'c20'])


len(labinfo_meancov)
len(labinfo_c20)


# push data
rd3.batch_update_one_attr(
    entity = 'rd3_freeze2_labinfo',
    attr = 'mean_cov',
    values = labinfo_meancov
)

rd3.batch_update_one_attr(
    entity = 'rd3_freeze2_labinfo',
    attr = 'c20',
    values = labinfo_c20
)


# cleanup
del config, rawdata, covdata, d, labinfo, rd3, l, labinfo_upload, labinfo_c20, labinfo_meancov, result
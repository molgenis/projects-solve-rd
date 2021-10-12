# '////////////////////////////////////////////////////////////////////////////
# ' FILE: experiment_id_mapping.py
# ' AUTHOR: David Ruvolo
# ' CREATED: 2021-05-19
# ' MODIFIED: 2021-05-19
# ' PURPOSE: map experiment ID into rd3_freeze*_files
# ' STATUS: working
# ' PACKAGES: NA
# ' COMMENTS: The purpose of this script is to map experimentID into the
# '          freeze files tables. This ID isn't currently available in the
# '          files tables and it is useful for adding cluster paths to RD3.
# '////////////////////////////////////////////////////////////////////////////

import molgenis.client as molgenis

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


# @title Flatten Files
# @description extract metadata of interest from rd3_freeze*_file
# @param data returned data from rd3_freeze*_file
# @return list of dictionaries
def flatten_files(data):
    out = []
    for d in data:
        tmp = {}
        tmp['EGA'] = d.get('EGA')
        tmp['sample_id'] = extract_nested_attr(d.get('samples'), 'id')
        tmp['sampleID'] = extract_nested_attr(d.get('samples'), 'sampleID')
        out.append(tmp)
    return out

# //////////////////////////////////////////////////////////////////////////////


# init session
token = '${molgenisToken}'
rd3 = molgenis.Session(url='https://solve-rd.gcc.rug.nl/api/', token=token)

# fetch data
rd3_freeze1_labs = rd3.get(
    entity='rd3_freeze1_labinfo',
    attributes='id,experimentID,sample',
    batch_size=10000
)

rd3_freeze1_files = rd3.get(
    entity='rd3_freeze1_file',
    attributes='EGA,samples',
    # num=1000,  # comment this when used in production
    batch_size=10000
)


print('Flattening entities (this may take some time)')

# flatten: rd3_freeze*_labinfo
print('Starting labinfo...')
rd3_freeze1_labs_flat = flatten_labinfo(data=rd3_freeze1_labs)

if len(rd3_freeze1_labs_flat):
    print('Flattend `labinfo`')
else:
    raise SystemExit('Unable to flatten `labinfo`. Check data structure')


# flatten rd3_freeze*_file`
print('Starting `file`....')
rd3_freeze1_files_flat = flatten_files(data=rd3_freeze1_files)

if len(rd3_freeze1_files):
    print('Flattend `file`')
else:
    raise SystemExit('Unable to flatten `file`. Check data structure')


# merge
print('Merging Lists....')
updated_freeze1_files = []
for file in rd3_freeze1_files_flat:
    if file['sample_id'] is not None:
        result = list(
            filter(lambda d: d['sample_id'] in file['sample_id'], rd3_freeze1_labs_flat))
        if len(result):
            file.update({
                'experimentID': result[0].get('experimentID')
            })
            updated_freeze1_files.append(file)


if len(updated_freeze1_files):
    print('Successfully added `experimentID`')
else:
    raise SystemExit('Failed to merge lists')


# Push to table
print('Updating table...')
for newfile in updated_freeze1_files:
    rd3.update_one(
        entity='rd3_freeze1_file',
        id_=newfile['EGA'],
        attr='experimentID',
        value=newfile['experimentID']
    )


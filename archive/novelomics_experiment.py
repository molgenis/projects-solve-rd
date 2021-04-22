# //////////////////////////////////////////////////////////////////////////////
# FILE: novelomics_shipment.py
# AUTHOR: David Ruvolo
# CREATED: 2021-02-24
# MODIFIED: 2021-03-11
# PURPOSE: process novel omics into Solve RD
# STATUS: working
# DEPENDENCIES: NA
# COMMENTS: 
# //////////////////////////////////////////////////////////////////////////////


import json
import molgenis.client as molgenis


# host = 'https://solve-rd.gcc.rug.nl/api/'
host = 'https://solve-rd-acc.gcc.rug.nl/api/'
input_entity = "rd3_portal_novelomics_experiment"
output_entity = 'rd3_novelomics-experiment'

token = '${molgenisToken}'
rd3 = molgenis.Session(url=host, token=token)
data = rd3.get(input_entity, batch_size=1000)
new = []


for d in data:
    t = {}
    t['EGA'] = d.get('file_ega_id')
    t['EGApath'] = d.get('file_path')
    t['name'] = d.get('file_name')
    t['md5'] = d.get('unencrypted_md5_checksum')
    t['typeFile'] = d.get('file_type')
    t['filegroupID'] = d.get('file_group_id')
    t['batchID'] = d.get('batch_id')
    t['experimentID'] = d.get('project_experiment_dataset_id')
    t['sampleID'] = d.get('sample_id')
    t['subjectID'] = d.get('subject_id')
    t['sequencingCentre'] = d.get('sequencing_center')
    t['sequencer'] = d.get('platform_model')
    t['libraryType'] = d.get('library_source')
    t['capture'] = d.get('library_strategy')
    t['librarySelectionId'] = d.get('library_selection')
    t['pairedNominalLength'] = d.get('paired_nominal_length')
    t['seqType'] = d.get('experiment_type')
    t['tissueType'] = d.get('tissue_type')
    t['materialType'] = d.get('sample_type')
    t['project_batch_id'] = d.get('project_batch_id')
    t['run_ega_id'] = d.get('run_ega_id')
    t['experiment_ega_id'] = d.get('experiment_ega_id')
    t['molgenis_id'] = d.get('molgenis_id')
    new.append(t)


for n in range(0, len(new), 1000):
    response = rd3._session.post(
        url=rd3._url + 'v2/' + output_entity,
        headers=rd3._get_token_header_with_content_type(),
        data=json.dumps({'entities': new[n:n+1000]})
    )
    if response.status_code == 201:
        print("Imported batch " + str(n) +
              " successfully (" + str(response.status_code) + ")")
    else:
        print("Failed to import batch " + str(n) +
              " (" + str(response.status_code) + ")")


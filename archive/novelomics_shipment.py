#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_shipment.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-03-11
#' MODIFIED: 2021-03-11
#' PURPOSE: process data from RD3 Portal into main RD3 area
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import json
import molgenis.client as molgenis


# host = 'https://solve-rd.gcc.rug.nl/api/'
host = 'https://solve-rd-acc.gcc.rug.nl/api/'
input_entity = "rd3_portal_novelomics_shipment"
output_entity = 'rd3_novelomics-shipment'


token = '${molgenisToken}'
rd3 = molgenis.Session(url=host, token=token)
data = rd3.get(input_entity, batch_size=1000)
new = []


for d in data:
    t = {}
    t['sampleID'] = d.get('sample_id')
    t['subjectID'] = d.get('participant_subject')
    t['seqType'] = d.get('type_of_analysis')
    t['tissueType'] = d.get('tissue_type')
    t['materialType'] = d.get('sample_type')
    t['experimentID'] = d.get('solve_rd_experiment_id')
    t['project_batch_id'] = d.get('batch')
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



final_response = rd3._session.delete(
    url = rd3._url + "v1/" + input_entity,
    headers = rd3._get_token_header_with_content_type()
) 

if final_response == 204:
    print("Cleared table")
else:
    print("Unable to clear table")
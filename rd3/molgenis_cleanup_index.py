"""Clean up elastic search index
FILE: molgenis_cleanup_index.py
AUTHOR: David Ruvolo
CREATED: 2024-01-11
MODIFIED: 2024-01-11
PURPOSE: clean up elastic search index table
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
from requests.exceptions import HTTPError
import molgenis.client as molgenis
from dotenv import load_dotenv
from tqdm import tqdm
load_dotenv()

rd3 = molgenis.Session(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# clear index action table
try:
    response = rd3.delete(entity='sys_idx_IndexAction')
    if response.status_code != 204:
        raise HTTPError(response.text)
    print('Deleted table')
except HTTPError as error:
    print(error)


# ///////////////////////////////////////

# clean index action group
try:
    response = rd3.delete(entity='sys_idx_IndexActionGroup')
    if response.status_code != 204:
        raise HTTPError(response.text)
    print('Deleted table')
except HTTPError as error:
    print(error)

# ///////////////////////////////////////

# clear index job execution table
try:
    response = rd3.delete(entity='sys_job_IndexJobExecution')
    if response.status_code != 204:
        raise HTTPError(response.text)
    print('Deleted table')
except HTTPError as error:
    print(error)

# if the previous step fails, retrieve a list of IDs and delete them
# QUERY='status=in=(FAILED,SUCCESS,PENDING)'
QUERY = 'status=in=(FAILED,SUCCESS)'

job_index = rd3.get(
    'sys_job_IndexJobExecution',
    q=QUERY,
    attributes='identifier',
    batch_size=10000
)

if len(job_index):
    print('Will attempt to delete', len(job_index), 'jobs')
    job_index_ids = [row['identifier'] for row in job_index]

    for batch in tqdm(range(0, len(job_index_ids), 1000)):
        response = rd3.delete_list(
            'sys_job_IndexJobExecution',
            entities=job_index_ids[batch:batch+1000]
        )


rd3.logout()

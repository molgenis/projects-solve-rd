#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_update_file_metadata.py
# AUTHOR: David Ruvolo
# CREATED: 2023-04-05
# MODIFIED: 2023-04-05
# PURPOSE: refresh solverd_files metadata
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: The purpose of this script is to refresh metadata in the files
# table. This includes filling in missing subject-, sample-, and experiment
# identifiers.
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import flattenDataset, timestamp
from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt,f
from os import environ
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Retrieve metadata and prep data objects

files = rd3.get(entity='solverd_files', batch_size=10000)
filesDT = flattenDataset(
  data=files,
  columnPatterns='subjectID|sampleID|experimentID'
)

filesDT = dt.Frame(filesDT)
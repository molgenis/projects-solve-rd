#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_update_percentage.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-02-23
#' MODIFIED: 2022-02-23
#' PURPOSE: Update tumor percentages for novel omics data
#' STATUS: in.progress
#' PACKAGES: NA
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from python._client import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, fread
from os import environ

load_dotenv()
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

db = Molgenis(url = host, token = token)

newData = 
samples = db.get(entity = 'rd3_novelomics_sample', batch_size = 10000)


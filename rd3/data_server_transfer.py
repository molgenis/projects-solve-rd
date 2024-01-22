# ///////////////////////////////////////////////////////////////////////////////
# FILE: data_server_transfer.py
# AUTHOR: David Ruvolo
# CREATED: 2024-01-17
# MODIFIED: 2024-01-18
# PURPOSE: transfer data between servers
# STATUS: stable; ongoing
# PACKAGES: **see below**
# COMMENTS: NA
# ///////////////////////////////////////////////////////////////////////////////

from os import environ
from dotenv import load_dotenv
from datatable import dt
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
load_dotenv()


# connect to RD3
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])


# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# transfer lookups

# transfer: organisations
orgs_dt = dt.Frame(rd3_prod.get('solverd_info_organisations'))
rd3_acc.import_dt('solverd_info_organisations', orgs_dt)

# transfer: data releases
release_raw = rd3_prod.get('solverd_info_datareleases')
release_dt = dt.Frame(flatten_data(release_raw, 'id'))
rd3_acc.import_dt('solverd_info_datareleases', release_dt)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Transfer Tables

# ~ 2a ~
# transfer subjects
print2('Transfering subjects data from PROD to ACC....')
subj_raw = rd3_prod.get('solverd_subjects', batch_size=10000)
subj_flat = flatten_data(subj_raw, 'subjectID|id|value')
subj_dt = dt.Frame(subj_flat)

rd3_acc.import_dt('solverd_subjects', subj_dt)

# ~ 2b ~
# transfer solved status portal data
print2('Transfering raw solved status from PROD to ACC...')
rd3_acc.delete('rd3_portal_recontact_solved')

solved_raw = rd3_prod.get('rd3_portal_recontact_solved', batch_size=10000)
solved_flat = flatten_data(solved_raw, 'id')
solved_dt = dt.Frame(solved_flat)

rd3_acc.import_dt('rd3_portal_recontact_solved', solved_dt)


rd3_acc.logout()
rd3_prod.logout()

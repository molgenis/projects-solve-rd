#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_acc_to_prod.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-18
#' MODIFIED: 2022-08-18
#' PURPOSE: transfer data from ACC to PROD
#' STATUS: stable
#' PACKAGES: datatable, dotenv, os
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from datatable import dt
from dotenv import load_dotenv
from os import environ as env
load_dotenv()

# connect via pyclient
rd3_acc = Molgenis(env['MOLGENIS_ACC_HOST'])
rd3_acc.login(env['MOLGENIS_ACC_USR'], env['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(env['MOLGENIS_PROD_HOST'])
rd3_prod.login(env['MOLGENIS_PROD_USR'], env['MOLGENIS_PROD_PWD'])

# pull from acc
data = dt.Frame(rd3_acc.get('rd3_noyesunknown'))
del data['_href']

# optional transformations
# data['description'] = dt.Frame([
#   value.strip()
#   for value in data['description'].to_list()[0]
#   if value is not None
# ])

# import into prod
rd3_prod.importDatatableAsCsv(pkg_entity='rd3_typeFile', data = data)

# disconnect
rd3_acc.logout()
rd3_prod.logout()
